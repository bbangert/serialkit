"""SerialLink lifecycle: start, dispatch, turns, reconnect, teardown.

These pin the parts that fail quietly when they regress: teardown ordering,
session identity across a reconnect, framer-reset-on-desync, and per-frame
hardening.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import Recorder

from serialkit import (
    Backoff,
    ConnectionLostError,
    DelimiterFramer,
    SerialLink,
)
from serialkit.testing import FakeLink

# Reconnects in these tests should be near-instant, not half a second.
FAST = Backoff(initial=0.01, factor=1.0, max_delay=0.01)


def make_link(link: FakeLink, handler: object, **kwargs: object) -> SerialLink:
    kwargs.setdefault("backoff", FAST)
    return SerialLink(
        connect=link.connect,
        framer=DelimiterFramer(b"\n"),
        handler=handler,  # type: ignore[arg-type]
        **kwargs,  # type: ignore[arg-type]
    )


# ---- start() failure propagation ----------------------------------------


async def test_start_propagates_connect_failure_and_supervises_nothing(
    handler: Recorder,
) -> None:
    async def failing_connect() -> tuple[object, object]:
        raise OSError("no such port")

    dev = SerialLink(
        connect=failing_connect,
        framer=DelimiterFramer(b"\n"),
        handler=handler,
    )
    with pytest.raises(OSError, match="no such port"):
        await dev.start()
    assert not dev.connected
    assert dev._monitor_task is None  # the reconnect loop never started


async def test_start_propagates_on_connect_failure_after_teardown(
    link: FakeLink,
) -> None:
    class BadHandshake(Recorder):
        async def on_connect(self) -> None:
            await super().on_connect()
            raise RuntimeError("handshake rejected")

    handler = BadHandshake()
    dev = make_link(link, handler)

    with pytest.raises(RuntimeError, match="handshake rejected"):
        await dev.start()

    assert not dev.connected
    assert dev._monitor_task is None
    assert handler.disconnects == [None]  # symmetric teardown ran
    # A failed start() supervises nothing: no reconnect attempt follows.
    await asyncio.sleep(0.05)
    assert link.connects == 1


async def test_start_twice_is_a_programming_error(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:
        with pytest.raises(RuntimeError, match="already started"):
            await dev.start()
    finally:
        await dev.stop()


# ---- a connect that never returns ---------------------------------------


async def test_hanging_connect_times_out_and_keeps_backing_off(
    handler: Recorder,
) -> None:
    """A device path that blocks on open must not wedge the reconnect loop:
    connect_timeout fires, and the supervisor keeps retrying."""
    link = FakeLink()
    connect = link.hang_connect()

    dev = SerialLink(
        connect=connect,
        framer=DelimiterFramer(b"\n"),
        handler=handler,
        backoff=FAST,
        connect_timeout=0.02,
    )
    with pytest.raises(TimeoutError):
        await dev.start()
    assert link.connects == 0


async def test_reconnect_survives_a_hanging_connect_attempt(
    link: FakeLink, handler: Recorder
) -> None:
    dev = SerialLink(
        connect=link.connect,
        framer=DelimiterFramer(b"\n"),
        handler=handler,
        backoff=FAST,
        connect_timeout=0.02,
    )
    await dev.start()
    try:
        link.hang_connect()  # every reconnect attempt now blocks on open
        link.drop()
        await asyncio.sleep(0.1)  # several timed-out attempts
        assert link.connects == 1
        assert not dev.connected

        link.resume_connect()  # the port comes back
        for _ in range(200):
            if dev.connected:
                break
            await asyncio.sleep(0.01)
        assert dev.connected
        assert link.connects == 2
    finally:
        await dev.stop()


# ---- fast-fail when not connected ---------------------------------------


async def test_send_fails_fast_before_start(link: FakeLink, handler: Recorder) -> None:
    dev = make_link(link, handler)
    with pytest.raises(ConnectionLostError):
        await dev.send(b"X\n")
    with pytest.raises(ConnectionLostError):
        dev.exchange()


async def test_send_fails_fast_after_stop(link: FakeLink, handler: Recorder) -> None:
    dev = make_link(link, handler)
    await dev.start()
    await dev.stop()
    with pytest.raises(ConnectionLostError):
        await dev.send(b"X\n")


# ---- dispatch: turns ----------------------------------------------------


async def test_on_turn_fires_once_per_frame_producing_chunk(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:
        link.rx(b"A\nB\nC\n")  # three frames, ONE chunk
        await asyncio.sleep(0.01)
        assert handler.frames == [b"A", b"B", b"C"]
        assert handler.turns == 1
        assert handler.turn_boundaries == [3]

        link.rx(b"D\n")
        link.rx(b"E\n")  # two chunks
        await asyncio.sleep(0.01)
        assert handler.turns == 3
        assert handler.turn_boundaries == [3, 4, 5]
    finally:
        await dev.stop()


async def test_on_turn_does_not_fire_for_a_zero_frame_chunk(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:
        link.rx(b"PARTIAL")  # no delimiter yet: no frame, no turn
        await asyncio.sleep(0.01)
        assert handler.frames == []
        assert handler.turns == 0

        link.rx(b"_REST\n")  # completes it
        await asyncio.sleep(0.01)
        assert handler.frames == [b"PARTIAL_REST"]
        assert handler.turns == 1
    finally:
        await dev.stop()


async def test_handler_without_optional_callbacks_still_works(
    link: FakeLink,
) -> None:
    """on_turn and on_disconnect are optional; a handler may define neither."""

    class Minimal:
        def __init__(self) -> None:
            self.frames: list[bytes] = []

        def on_frame(self, frame: bytes) -> None:
            self.frames.append(frame)

        async def on_connect(self) -> None:
            pass

    handler = Minimal()
    dev = make_link(link, handler)
    await dev.start()
    try:
        link.rx(b"A\n")
        await asyncio.sleep(0.01)
        assert handler.frames == [b"A"]
    finally:
        await dev.stop()  # teardown must not trip over the missing callback


# ---- dispatch: hardening ------------------------------------------------


async def test_frame_exception_hardening_keeps_dispatch_alive(
    link: FakeLink,
) -> None:
    """An on_frame crash on one frame must not stop the next frame in the same
    turn from routing, nor kill the dispatch task."""

    class Crashy(Recorder):
        def on_frame(self, frame: bytes) -> None:
            if frame == b"BAD":
                raise RuntimeError("boom")
            super().on_frame(frame)

    handler = Crashy()
    dev = make_link(link, handler)
    await dev.start()
    try:
        link.rx(b"BAD\nGOOD\n")
        await asyncio.sleep(0.01)

        assert handler.frames == [b"GOOD"]
        assert len(dev.frame_errors) == 1
        assert dev.frame_errors[0][0] == b"BAD"
        assert handler.turns == 1  # the crashed frame still counted as one
        assert dev.connected  # dispatch survived
        assert link.connects == 1  # and no reconnect was triggered
    finally:
        await dev.stop()


async def test_frame_errors_is_bounded(link: FakeLink) -> None:
    """An unbounded list grows forever under multi-month uptimes."""

    class AlwaysCrashes(Recorder):
        def on_frame(self, frame: bytes) -> None:
            raise RuntimeError("boom")

    dev = make_link(link, AlwaysCrashes())
    await dev.start()
    try:
        link.rx(b"".join(b"F%d\n" % i for i in range(200)))
        await asyncio.sleep(0.05)
        assert len(dev.frame_errors) == 64
        assert dev.frame_errors[-1][0] == b"F199"  # newest kept, oldest evicted
    finally:
        await dev.stop()


async def test_runtime_resets_framer_and_routes_pre_desync_frames(
    link: FakeLink, handler: Recorder
) -> None:
    dev = SerialLink(
        connect=link.connect,
        framer=DelimiterFramer(b"\n", max_frame=8),
        handler=handler,
        backoff=FAST,
    )
    await dev.start()
    try:
        # GOOD completes, then 20 undelimited bytes overflow -> ResyncError
        # whose .frames still carries GOOD.
        link.rx(b"GOOD\n" + b"X" * 20)
        await asyncio.sleep(0.01)

        assert handler.frames == [b"GOOD"]  # pre-desync frame was routed
        assert dev.frame_errors[-1][0] == b""  # the desync itself is recorded
        assert dev.connected  # dispatch survived; no reconnect
        assert link.connects == 1

        # The runtime reset the framer, so the next frame parses cleanly.
        link.rx(b"NEXT\n")
        await asyncio.sleep(0.01)
        assert handler.frames == [b"GOOD", b"NEXT"]
    finally:
        await dev.stop()


# ---- framer prototype ---------------------------------------------------


async def test_framer_prototype_is_copied_not_shared(
    link: FakeLink, handler: Recorder
) -> None:
    """The framer is a prototype instance: per-connection copies mean no
    residual buffer is ever inherited, and no staticmethod factory is needed."""
    prototype = DelimiterFramer(b"\n")
    dev = SerialLink(
        connect=link.connect,
        framer=prototype,
        handler=handler,
        backoff=FAST,
    )
    await dev.start()
    try:
        first = dev._framer
        assert first is not prototype

        link.rx(b"HALF")  # residual left in the first framer
        await asyncio.sleep(0.01)
        link.drop()
        for _ in range(200):
            if link.connects >= 2:
                break
            await asyncio.sleep(0.01)

        assert dev._framer is not first
        link.rx(b"WHOLE\n")
        await asyncio.sleep(0.01)
        # No "HALFWHOLE": the new connection started from an empty buffer.
        assert handler.frames == [b"WHOLE"]
    finally:
        await dev.stop()


# ---- reconnect ----------------------------------------------------------


@pytest.mark.parametrize("abrupt", [False, True], ids=["eof", "unplug"])
async def test_reconnect_reruns_on_connect(
    link: FakeLink, handler: Recorder, abrupt: bool
) -> None:
    """Both drop shapes recover: EOF (socket FIN) and a raised OSError (a real
    serial device being unplugged)."""
    dev = make_link(link, handler)
    await dev.start()
    try:
        assert handler.connects == 1
        link.drop(abrupt=abrupt)

        for _ in range(200):
            if link.connects >= 2 and dev.connected:
                break
            await asyncio.sleep(0.01)

        assert link.connects == 2
        assert dev.connected
        assert dev.session == 2
        assert handler.connects == 2  # on_connect re-ran: the driver re-queries
        assert len(handler.disconnects) == 1
        assert isinstance(handler.disconnects[0], ConnectionLostError)

        link.rx(b"ALIVE\n")
        await asyncio.sleep(0.01)
        assert handler.frames == [b"ALIVE"]
    finally:
        await dev.stop()


async def test_write_queued_behind_pacing_is_abandoned_across_a_reconnect(
    link: FakeLink, handler: Recorder
) -> None:
    """A caller stuck behind pacing when the link drops must NOT wake up
    mid-reconnect and write its stale frame onto the NEW session."""
    from serialkit import Pacing

    dev = SerialLink(
        connect=link.connect,
        framer=DelimiterFramer(b"\n"),
        handler=handler,
        pacing=Pacing(min_interval=0.05),
        backoff=FAST,
    )
    await dev.start()
    try:
        await dev.send(b"A\n")  # arms a 50ms pacing debt
        task = asyncio.ensure_future(dev.send(b"B\n"))  # queues behind it
        await asyncio.sleep(0.005)
        assert link.sent == [b"A\n"]

        link.drop()  # reconnect completes during B's pacing wait

        with pytest.raises(ConnectionLostError):
            await task

        await asyncio.sleep(0.1)
        assert link.connects == 2
        assert link.writers[0].written == [b"A\n"]
        assert link.writers[1].written == []  # B never landed on session 2
    finally:
        await dev.stop()


async def test_on_connect_failure_on_reconnect_retries_with_backoff(
    link: FakeLink,
) -> None:
    class FlakyHandshake(Recorder):
        async def on_connect(self) -> None:
            await super().on_connect()
            if 1 < self.connects < 4:
                raise RuntimeError("handshake rejected")

    handler = FlakyHandshake()
    dev = make_link(link, handler)
    await dev.start()
    try:
        link.drop()
        for _ in range(300):
            if dev.connected and handler.connects >= 4:
                break
            await asyncio.sleep(0.01)
        assert handler.connects == 4  # two rejections, then success
        assert dev.connected
    finally:
        await dev.stop()


# ---- stop() -------------------------------------------------------------


async def test_stop_is_symmetric_and_never_reconnects(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    await dev.start()
    await dev.stop()

    assert not dev.connected
    assert link.writer.closed
    assert handler.disconnects == [None]  # None, not an exception: not a drop

    await asyncio.sleep(0.05)
    assert link.connects == 1


async def test_in_flight_waits_fail_before_on_disconnect(
    link: FakeLink,
) -> None:
    """Teardown ordering: by the time the driver is told the link went away,
    every in-flight wait is already resolved. A driver that tidies up in
    on_disconnect must not find a waiter still pending behind it.

    (The awaiting *task* resumes later — asyncio never resumes one
    synchronously — so what a driver can actually observe is the future's
    state, which is what this pins.)
    """
    observed: list[BaseException | None] = []

    class Ordered(Recorder):
        def __init__(self) -> None:
            super().__init__()
            self.waiter: asyncio.Future[bytes] | None = None

        def on_disconnect(self, exc: Exception | None) -> None:
            super().on_disconnect(exc)
            assert self.waiter is not None
            assert self.waiter.done(), "waiter still pending at on_disconnect"
            observed.append(self.waiter.exception())

    handler = Ordered()
    dev = make_link(link, handler)
    await dev.start()
    try:
        waiter = dev.expect(lambda f: False, timeout=30.0)
        handler.waiter = waiter  # type: ignore[assignment]

        link.drop()
        with pytest.raises(ConnectionLostError):
            await waiter

        assert len(observed) == 1
        assert isinstance(observed[0], ConnectionLostError)
    finally:
        await dev.stop()


async def test_stop_is_idempotent(link: FakeLink, handler: Recorder) -> None:
    dev = make_link(link, handler)
    await dev.start()
    await dev.stop()
    await dev.stop()
    assert handler.disconnects == [None]


# ---- writes -------------------------------------------------------------


async def test_writes_await_drain(link: FakeLink, handler: Recorder) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:
        await dev.send(b"PING\n")
        assert link.writer.drains >= 1
    finally:
        await dev.stop()


async def test_write_failure_reaches_the_caller(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:
        link.fail_writes()
        with pytest.raises(OSError):
            await dev.send(b"BOOM\n")
    finally:
        await dev.stop()


async def test_silent_write_failure_is_invisible_to_the_caller(
    link: FakeLink, handler: Recorder
) -> None:
    """The fd-transport shape: send() reports success for a frame that never
    went out. The kit cannot detect this — only liveness can, which is why a
    transactional device needs FailureCount."""
    dev = make_link(link, handler)
    await dev.start()
    try:
        link.fail_writes(silent=True)
        await dev.send(b"LOST\n")  # no exception
        assert link.writer.discarded == [b"LOST\n"]
        assert link.sent == []
    finally:
        await dev.stop()


# ---- on_connect runs with frames already flowing ------------------------


async def test_on_connect_can_wait_for_frames(link: FakeLink) -> None:
    """on_connect runs while dispatch is already live, so a driver can send
    and wait inside it — which is what makes it the place for a full
    re-query."""

    class Handshaker(Recorder):
        def __init__(self) -> None:
            super().__init__()
            self.link: SerialLink | None = None
            self.reply: bytes | None = None

        async def on_connect(self) -> None:
            await super().on_connect()
            assert self.link is not None
            self.reply = await self.link.confirm(
                nudge=b"ID?\n",
                match=lambda f: f.startswith(b"ID"),
                timeout=1.0,
            )

    handler = Handshaker()
    dev = make_link(link, handler)
    handler.link = dev
    # The answer arrives sandwiched between unsolicited frames, all in one
    # chunk — one dispatch turn.
    link.respond({b"ID?\n": b"NOISE\nID42\nMORE\n"})

    await dev.start()  # would hang if frames weren't flowing during on_connect
    try:
        assert handler.reply == b"ID42"
        await asyncio.sleep(0.01)
        # expect() observes without consuming: ID42 still reached on_frame.
        assert handler.frames == [b"NOISE", b"ID42", b"MORE"]
    finally:
        await dev.stop()
