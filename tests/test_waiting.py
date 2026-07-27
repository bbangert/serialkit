"""expect / confirm / exchange: arming, routing order, and RX anchoring.

This is the subtle part of the kit. Arming before the send, observe-vs-consume,
and anchoring a reply by arrival order are each a place where getting it
backwards fails silently — a dropped state update, or a reply read as the
answer to the wrong command.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import Recorder

from serialkit import (
    Backoff,
    CommandTimeoutError,
    ConnectionLostError,
    DelimiterFramer,
    Pacing,
    ProtocolError,
    SerialLink,
)
from serialkit.testing import FakeLink

FAST = Backoff(initial=0.01, factor=1.0, max_delay=0.01)


def make_link(link: FakeLink, handler: object, **kwargs: object) -> SerialLink:
    kwargs.setdefault("backoff", FAST)
    return SerialLink(
        connect=link.connect,
        framer=DelimiterFramer(b"\n"),
        handler=handler,  # type: ignore[arg-type]
        **kwargs,  # type: ignore[arg-type]
    )


def starts_with(prefix: bytes) -> object:
    return lambda frame: frame.startswith(prefix)


# ---- expect: armed at call time -----------------------------------------


async def test_expect_armed_before_send_catches_a_same_chunk_reply(
    link: FakeLink, handler: Recorder
) -> None:
    """The whole point of arming being synchronous: a device that replies
    inside the same read chunk as the write must not outrun the waiter."""
    dev = make_link(link, handler)
    link.respond({b"POW?\n": b"POW1\n"})
    await dev.start()
    try:
        waiter = dev.expect(starts_with(b"POW"), timeout=1.0)  # type: ignore[arg-type]
        await dev.send(b"POW?\n")
        assert await waiter == b"POW1"
    finally:
        await dev.stop()


async def test_expect_observes_it_does_not_consume(
    link: FakeLink, handler: Recorder
) -> None:
    """A frame satisfying an expect predicate is normally ALSO a state event
    the driver must apply: the frame confirming a power-on is itself the
    power-state report. Consuming it here would silently drop state updates."""
    dev = make_link(link, handler)
    await dev.start()
    try:
        waiter = dev.expect(starts_with(b"POW"), timeout=1.0)  # type: ignore[arg-type]
        link.rx(b"POW1\n")
        assert await waiter == b"POW1"
        await asyncio.sleep(0.01)
        assert handler.frames == [b"POW1"]  # reached on_frame as well
        assert handler.turns == 1
    finally:
        await dev.stop()


async def test_expect_ignores_frames_that_do_not_match(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:
        waiter = dev.expect(starts_with(b"POW"), timeout=1.0)  # type: ignore[arg-type]
        link.rx(b"VOL-30\nMUT0\nPOW1\n")
        assert await waiter == b"POW1"
        assert handler.frames == [b"VOL-30", b"MUT0", b"POW1"]
    finally:
        await dev.stop()


async def test_expect_times_out(link: FakeLink, handler: Recorder) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:
        with pytest.raises(CommandTimeoutError):
            await dev.expect(starts_with(b"POW"), timeout=0.02)  # type: ignore[arg-type]
    finally:
        await dev.stop()


async def test_expect_timeout_clock_starts_at_arm_time(
    link: FakeLink, handler: Recorder
) -> None:
    """Arming before the send is only useful if the window it covers starts
    there too — otherwise a slow send eats into the caller's budget."""
    dev = make_link(link, handler)
    await dev.start()
    try:
        waiter = dev.expect(starts_with(b"POW"), timeout=0.05)
        await asyncio.sleep(0.08)  # the arm-time window has already elapsed
        link.rx(b"POW1\n")
        with pytest.raises(CommandTimeoutError):
            await waiter
    finally:
        await dev.stop()


async def test_every_matching_waiter_resolves(
    link: FakeLink, handler: Recorder
) -> None:
    """expect() does not claim, so one frame can satisfy several waiters."""
    dev = make_link(link, handler)
    await dev.start()
    try:
        a = dev.expect(starts_with(b"POW"), timeout=1.0)  # type: ignore[arg-type]
        b = dev.expect(lambda f: b"1" in f, timeout=1.0)
        link.rx(b"POW1\n")
        assert await a == b"POW1"
        assert await b == b"POW1"
        assert handler.frames == [b"POW1"]
    finally:
        await dev.stop()


async def test_a_raising_predicate_is_treated_as_no_match(
    link: FakeLink, handler: Recorder
) -> None:
    """Predicates are driver code on the dispatch task; one bad one must not
    take the read loop down."""
    dev = make_link(link, handler)
    await dev.start()
    try:

        def boom(frame: bytes) -> bool:
            raise ValueError("bad predicate")

        bad = dev.expect(boom, timeout=0.05)
        good = dev.expect(starts_with(b"POW"), timeout=1.0)  # type: ignore[arg-type]
        link.rx(b"POW1\n")

        assert await good == b"POW1"
        with pytest.raises(CommandTimeoutError):
            await bad
        assert dev.connected
        assert handler.frames == [b"POW1"]
    finally:
        await dev.stop()


# ---- confirm ------------------------------------------------------------


async def test_confirm_sends_and_returns_the_confirming_frame(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    link.respond({b"POW1;\n": b"POW1\n"})
    await dev.start()
    try:
        frame = await dev.confirm(
            nudge=b"POW1;\n",
            match=starts_with(b"POW"),
            timeout=1.0,  # type: ignore[arg-type]
        )
        assert frame == b"POW1"
        assert link.sent == [b"POW1;\n"]
        assert handler.frames == [b"POW1"]  # still applied as a state event
    finally:
        await dev.stop()


async def test_confirm_retries_resend_the_nudge(
    link: FakeLink, handler: Recorder
) -> None:
    """retries=1 covers a device asleep in standby: its MCU consumes the first
    frame waking up, so the second is the one that acts. This is a documented
    requirement on real hardware, not a reliability retry."""
    dev = make_link(link, handler)
    writes: list[bytes] = []

    def wake_on_second(data: bytes) -> None:
        writes.append(data)
        if len(writes) == 2:  # the first frame is eaten by the wake-up
            link.rx(b"POW1\n")

    link.on_write = wake_on_second
    await dev.start()
    try:
        frame = await dev.confirm(
            nudge=b"POW1;\n",
            match=starts_with(b"POW"),  # type: ignore[arg-type]
            timeout=0.05,
            retries=1,
        )
        assert frame == b"POW1"
        assert link.sent == [b"POW1;\n", b"POW1;\n"]
    finally:
        await dev.stop()


async def test_confirm_raises_when_every_attempt_times_out(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:
        with pytest.raises(CommandTimeoutError):
            await dev.confirm(
                nudge=b"POW1;\n",
                match=starts_with(b"POW"),  # type: ignore[arg-type]
                timeout=0.02,
                retries=2,
            )
        assert link.sent == [b"POW1;\n"] * 3  # 1 attempt + 2 retries
    finally:
        await dev.stop()


async def test_confirm_cannot_pass_vacuously(link: FakeLink, handler: Recorder) -> None:
    """A frame that has not arrived yet cannot already be true. Power is
    already on and stays on; with nothing on the wire, confirm must fail rather
    than report success into a void — the silently-dead-link case."""
    dev = make_link(link, handler)
    await dev.start()
    try:
        link.rx(b"POW1\n")  # the desired state is ALREADY reported
        await asyncio.sleep(0.01)
        link.on_write = None  # ...and the device then goes silent

        with pytest.raises(CommandTimeoutError):
            await dev.confirm(
                nudge=b"POW1;\n",
                match=starts_with(b"POW"),  # type: ignore[arg-type]
                timeout=0.02,
            )
    finally:
        await dev.stop()


async def test_report_error_fails_waiters_immediately(
    link: FakeLink,
) -> None:
    """A device error frame makes the outstanding wait pointless: raise now
    instead of waiting out a multi-second timeout."""

    class ErrorRouting(Recorder):
        def __init__(self) -> None:
            super().__init__()
            self.link: SerialLink | None = None

        def on_frame(self, frame: bytes) -> None:
            super().on_frame(frame)
            if frame.startswith(b"!R"):
                assert self.link is not None
                self.link.report_error(ProtocolError(frame.decode()))

    handler = ErrorRouting()
    dev = make_link(link, handler)
    handler.link = dev
    link.respond({b"POW1;\n": b"!RPOW1\n"})
    await dev.start()
    try:
        started = asyncio.get_running_loop().time()
        with pytest.raises(ProtocolError):
            await dev.confirm(
                nudge=b"POW1;\n",
                match=starts_with(b"POW"),  # type: ignore[arg-type]
                timeout=30.0,
            )
        assert asyncio.get_running_loop().time() - started < 1.0
    finally:
        await dev.stop()


async def test_reconnect_fails_in_flight_waiters(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:
        waiter = dev.expect(starts_with(b"POW"), timeout=30.0)  # type: ignore[arg-type]
        link.drop()
        with pytest.raises(ConnectionLostError):
            await waiter
    finally:
        await dev.stop()


# ---- exchange -----------------------------------------------------------


async def test_exchange_returns_the_next_frame_and_claims_it(
    link: FakeLink, handler: Recorder
) -> None:
    """Unlike expect(), an exchange consumes. When a reply is only decodable
    against the outstanding command, on_frame has nothing useful to do with
    it."""
    dev = make_link(link, handler)
    link.respond({b"Q\n": b"ANSWER\n"})
    await dev.start()
    try:
        async with dev.exchange() as ex:
            await ex.send(b"Q\n")
            assert await ex.next(timeout=1.0) == b"ANSWER"
        await asyncio.sleep(0.01)
        assert handler.frames == []  # claimed, so on_frame never saw it
        assert handler.turns == 0
    finally:
        await dev.stop()


async def test_exchange_queues_a_frame_arriving_before_the_await(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    link.respond({b"Q\n": b"ANSWER\n"})
    await dev.start()
    try:
        async with dev.exchange() as ex:
            await ex.send(b"Q\n")
            await asyncio.sleep(0.02)  # the reply lands before next() is called
            assert await ex.next(timeout=1.0) == b"ANSWER"
    finally:
        await dev.stop()


async def test_exchange_is_exclusive(link: FakeLink, handler: Recorder) -> None:
    dev = make_link(link, handler)
    order: list[str] = []

    async def round_trip(name: str) -> None:
        async with dev.exchange() as ex:
            order.append(f"open:{name}")
            await ex.send(b"Q\n")
            link.rx(b"ANSWER\n")
            await ex.next(timeout=1.0)
            order.append(f"close:{name}")

    await dev.start()
    try:
        await asyncio.gather(round_trip("a"), round_trip("b"))
        # Never interleaved: one exchange holds the wire at a time.
        assert order in (
            ["open:a", "close:a", "open:b", "close:b"],
            ["open:b", "close:b", "open:a", "close:a"],
        )
    finally:
        await dev.stop()


async def test_frames_before_the_first_send_route_normally(
    link: FakeLink, handler: Recorder
) -> None:
    """An open exchange only claims once something is outstanding; before the
    first send nothing is owed, so unsolicited frames reach on_frame."""
    dev = make_link(link, handler)
    await dev.start()
    try:
        async with dev.exchange():
            link.rx(b"UNSOLICITED\n")
            await asyncio.sleep(0.01)
            assert handler.frames == [b"UNSOLICITED"]
    finally:
        await dev.stop()


async def test_exchange_next_before_send_is_a_programming_error(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:
        async with dev.exchange() as ex:
            with pytest.raises(RuntimeError, match="before send"):
                await ex.next(timeout=1.0)
    finally:
        await dev.stop()


async def test_exchange_next_times_out(link: FakeLink, handler: Recorder) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:
        async with dev.exchange() as ex:
            await ex.send(b"Q\n")
            with pytest.raises(CommandTimeoutError):
                await ex.next(timeout=0.02)
    finally:
        await dev.stop()


async def test_unconsumed_claimed_frames_are_released_on_close(
    link: FakeLink, handler: Recorder
) -> None:
    """Anything the exchange claimed but never read is handed back to normal
    routing rather than vanishing."""
    dev = make_link(link, handler)
    await dev.start()
    try:
        async with dev.exchange() as ex:
            await ex.send(b"Q\n")
            link.rx(b"ANSWER\nEXTRA\n")
            assert await ex.next(timeout=1.0) == b"ANSWER"
        await asyncio.sleep(0.01)
        assert handler.frames == [b"EXTRA"]
        assert handler.turns == 1
    finally:
        await dev.stop()


# ---- the late reply -----------------------------------------------------


async def test_late_reply_to_a_timed_out_exchange_is_discarded(
    link: FakeLink, handler: Recorder
) -> None:
    """The classic serial desync, structurally.

    Exchange A times out. B opens and queues behind pacing. A's answer finally
    arrives while B is still waiting its pacing turn — before B's frame is even
    on the wire. Under content matching this is indistinguishable from B's
    answer — when replies carry no function identifier the two frames are
    byte-identical — so B would decode it with A's decoder and write a
    plausible wrong value into state.

    The anchor is recorded at write time, after pacing, so A's answer lands
    behind it and is discarded.
    """
    dev = make_link(link, handler, pacing=Pacing(min_interval=0.08))
    await dev.start()
    try:
        async with dev.exchange() as ex_a:
            await ex_a.send(b"QA\n")
            with pytest.raises(CommandTimeoutError):
                await ex_a.next(timeout=0.02)

        # B's send is now queued behind A's 80ms pacing debt.
        async with dev.exchange() as ex_b:
            send_b = asyncio.ensure_future(ex_b.send(b"QB\n"))
            await asyncio.sleep(0.01)
            assert link.sent == [b"QA\n"]  # B is NOT on the wire yet

            link.rx(b"LATE_ANSWER_TO_A\n")  # A's reply finally shows up
            await asyncio.sleep(0.01)
            await send_b
            assert link.sent == [b"QA\n", b"QB\n"]

            # B must not read A's answer as its own.
            with pytest.raises(CommandTimeoutError):
                await ex_b.next(timeout=0.05)

            # A reply that genuinely follows a send still works.
            await ex_b.send(b"QB2\n")
            link.rx(b"ANSWER_TO_B\n")
            assert await ex_b.next(timeout=0.5) == b"ANSWER_TO_B"
    finally:
        await dev.stop()


async def test_reconnect_during_an_open_exchange_does_not_wedge_the_wire(
    link: FakeLink, handler: Recorder
) -> None:
    """The exclusivity lock must be released and the waiter failed, or the
    next exchange deadlocks forever."""
    dev = make_link(link, handler)
    await dev.start()
    try:
        with pytest.raises(ConnectionLostError):
            async with dev.exchange() as ex:
                await ex.send(b"Q\n")
                link.drop()
                await ex.next(timeout=30.0)

        for _ in range(200):
            if dev.connected and link.connects >= 2:
                break
            await asyncio.sleep(0.01)

        # The wire is free: a fresh exchange acquires it immediately.
        link.respond({b"Q2\n": b"OK\n"})
        async with asyncio.timeout(1.0):
            async with dev.exchange() as ex2:
                await ex2.send(b"Q2\n")
                assert await ex2.next(timeout=1.0) == b"OK"
    finally:
        await dev.stop()


async def test_caller_cancellation_releases_the_wire_lock(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler)
    await dev.start()
    try:

        async def hold() -> None:
            async with dev.exchange() as ex:
                await ex.send(b"Q\n")
                await ex.next(timeout=30.0)

        task = asyncio.ensure_future(hold())
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        link.respond({b"Q2\n": b"OK\n"})
        async with asyncio.timeout(1.0):
            async with dev.exchange() as ex2:
                await ex2.send(b"Q2\n")
                assert await ex2.next(timeout=1.0) == b"OK"
    finally:
        await dev.stop()
