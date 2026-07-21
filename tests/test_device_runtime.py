"""SerialDevice runtime scenarios: handshake-while-frames-flow,
rebuild-on-reconnect, sony caller-task ordering, denon burst notification
coalescing, probe watchdog, stop()."""

from __future__ import annotations

import asyncio

import pytest

from serialkit import (
    ConnectionLostError,
    ProbeSpec,
    match_prefix,
)

from conftest import DictDevice, FakeLink


# ---------------------------------------------------------------- (a) ----

async def test_on_connect_handshake_completes_while_frames_flow() -> None:
    """on_connect() runs while the dispatch task is already live: a
    request() from inside the handshake resolves even though unsolicited
    frames arrive before, between, and after its answer."""
    link = FakeLink(preload=[b"NOISE_PRE\n"])

    class HandshakeDevice(DictDevice):
        def __init__(self, connect) -> None:
            super().__init__(connect)
            self.handshake_response: bytes | None = None
            self.unsolicited: list[bytes] = []

        async def on_connect(self) -> None:
            self.handshake_response = await self.request(
                b"ID?\n", match_prefix(b"ID"), timeout=1.0)

        def on_frame(self, frame: bytes) -> None:
            if not self.pending.feed(frame):
                self.unsolicited.append(frame)

    def echo(data: bytes) -> None:
        if data == b"ID?\n":
            # The answer arrives sandwiched between unsolicited frames,
            # all in one chunk (one dispatch turn).
            link.rx(b"NOISE_MID\nID42\nNOISE_POST\n")

    link.on_write = echo
    dev = HandshakeDevice(link.connect)
    await dev.start()   # start() awaits on_connect; would hang/timeout if
    try:                # frames were not flowing during the handshake
        assert dev.handshake_response == b"ID42"
        assert dev.connected
        await asyncio.sleep(0.01)
        assert dev.unsolicited == [b"NOISE_PRE", b"NOISE_MID", b"NOISE_POST"]
    finally:
        await dev.stop()


# ---------------------------------------------------------------- (b) ----

async def test_reconnect_rebuilds_state_and_fails_old_pending(
    link: FakeLink,
) -> None:
    """On connection loss: fail_all pending -> on_disconnect(exc) ->
    notify(None) -> backoff -> fresh framer + fresh make_state() state ->
    on_connect -> notify. Subscribers see None, then a fresh snapshot with
    no stale fields."""

    class VolDevice(DictDevice):
        def __init__(self, connect) -> None:
            super().__init__(connect)
            self.disconnects: list[Exception | None] = []

        def on_frame(self, frame: bytes) -> None:
            if frame.startswith(b"VOL"):
                self.state["volume"] = int(frame[3:])
                self.notify()
            self.pending.feed(frame)

        def on_disconnect(self, exc: Exception | None) -> None:
            self.disconnects.append(exc)

    dev = VolDevice(link.connect)
    snapshots: list = []
    dev.subscribe(snapshots.append)
    await dev.start()
    try:
        # One tick so start()'s coalesced initial notify flushes before any
        # frame arrives (otherwise it merges with the first frame's notify).
        await asyncio.sleep(0)
        first_framer = dev._framer
        link.rx(b"VOL42\n")
        await asyncio.sleep(0.01)
        assert dev.state == {"volume": 42}

        # A request is in flight when the connection drops...
        stale = asyncio.ensure_future(
            dev.request(b"Q?\n", match_prefix(b"NEVER"), timeout=5.0))
        await asyncio.sleep(0.01)
        link.drop()  # EOF

        # ...and is failed by the reconnect teardown, not left hanging.
        with pytest.raises(ConnectionLostError):
            await stale

        await asyncio.sleep(0.05)  # tiny backoff (0.01) + reconnect
        assert link.connects == 2
        assert dev.connected

        # Fresh state: no stale volume field survives the reconnect.
        assert dev.state == {}
        assert dev._framer is not first_framer

        # Subscriber saw: initial {}, {volume:42}, None, fresh {}.
        assert snapshots == [{}, {"volume": 42}, None, {}]

        # on_disconnect got the exception (not None: this was not a stop).
        assert len(dev.disconnects) == 1
        assert isinstance(dev.disconnects[0], ConnectionLostError)

        # The rebuilt session is live end-to-end.
        task = asyncio.ensure_future(
            dev.request(b"VOL?\n", match_prefix(b"VOL"), timeout=1.0))
        await asyncio.sleep(0.01)
        link.rx(b"VOL7\n")
        assert await task == b"VOL7"
        assert dev.state == {"volume": 7}
    finally:
        await dev.stop()


# ---------------------------------------------------------------- (c) ----

async def test_sony_pattern_caller_mutation_orders_after_dispatch_turn(
    link: FakeLink,
) -> None:
    """Sony pattern: command methods do resp = await self.request(...) then
    mutate state + notify from the CALLER task. The caller only resumes after
    the entire dispatch turn that resolved its future — so an unsolicited
    frame arriving in the same chunk as the answer is applied to state BEFORE
    the caller's post-response mutation."""
    order: list[str] = []

    class SonyDevice(DictDevice):
        max_in_flight = 1

        def on_frame(self, frame: bytes) -> None:
            order.append(f"frame:{frame.decode()}")
            if frame.startswith(b"EVT"):
                self.state["event"] = frame.decode()
                self.notify()
                return
            self.pending.feed(frame)

        async def set_power(self) -> None:
            resp = await self.request(
                b"POWER\n", match_prefix(b"ANS"), timeout=1.0)
            order.append("cmd:resume")
            self.state["power"] = resp.decode()
            self.notify()

    dev = SonyDevice(link.connect)
    await dev.start()
    try:
        task = asyncio.ensure_future(dev.set_power())
        await asyncio.sleep(0.01)
        assert link.sent == [b"POWER\n"]

        # Answer + trailing unsolicited event arrive in ONE dispatch turn.
        link.rx(b"ANS_OK\nEVT_LATE\n")
        await task

        # Both frames were dispatched before the caller resumed.
        assert order == ["frame:ANS_OK", "frame:EVT_LATE", "cmd:resume"]
        # Caller-side mutation landed on top of the event's mutation.
        assert dev.state == {"event": "EVT_LATE", "power": "ANS_OK"}
    finally:
        await dev.stop()


# ---------------------------------------------------------------- (d) ----

async def test_denon_query_burst_notification_count(link: FakeLink) -> None:
    """A sequential query_state() burst of 8 awaited requests produces 8
    subscriber notifications — each response arrives in its own dispatch turn,
    so dispatch-turn coalescing does NOT collapse a sequential burst (it does
    not replace batch() for this pattern). The contrast case shows the
    coalescing working as designed when all answers share one turn."""
    burst = 8

    class DenonDevice(DictDevice):
        def on_frame(self, frame: bytes) -> None:
            self.state[frame[:2].decode()] = frame[2:].decode()
            self.notify()
            self.pending.feed(frame)

        async def query_state(self) -> None:
            for i in range(burst):
                await self.request(
                    f"Q{i}?\n".encode(), match_prefix(f"R{i}".encode()),
                    timeout=1.0)

    def echo(data: bytes) -> None:
        link.rx(f"R{data[1:2].decode()}VAL\n".encode())

    link.on_write = echo
    dev = DenonDevice(link.connect)
    notifications: list = []
    dev.subscribe(notifications.append)
    await dev.start()
    try:
        await asyncio.sleep(0)  # drain start()'s initial notify
        notifications.clear()

        # Sequential burst: each await resumes only after its response's
        # dispatch turn, so every response is its own turn.
        await dev.query_state()
        await asyncio.sleep(0.01)
        sequential_count = len(notifications)
        assert None not in notifications
        # MEASURED, not aspirational: 8 requests -> 8 notifications.
        assert sequential_count == burst
        assert dev.state == {f"R{i}": "VAL" for i in range(burst)}

        # Contrast: 8 concurrent requests whose answers land in ONE chunk
        # (one dispatch turn) coalesce to a single notification.
        link.on_write = None
        notifications.clear()
        tasks = [
            asyncio.ensure_future(dev.request(
                f"Q{i}?\n".encode(), match_prefix(f"R{i}".encode()),
                timeout=1.0))
            for i in range(burst)
        ]
        await asyncio.sleep(0.01)
        link.rx(b"".join(f"R{i}NEW\n".encode() for i in range(burst)))
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.01)
        assert len(notifications) == 1
    finally:
        await dev.stop()


async def test_batch_coalesces_sequential_burst_to_one_notification(
    link: FakeLink,
) -> None:
    """batch() sugar: a sequential burst wrapped in `with device.batch()`
    delivers ONE notification instead of one per response."""
    burst = 8

    class DenonDevice(DictDevice):
        def on_frame(self, frame: bytes) -> None:
            self.state[frame[:2].decode()] = frame[2:].decode()
            self.notify()
            self.pending.feed(frame)

        async def query_state(self) -> None:
            with self.batch():
                for i in range(burst):
                    await self.request(
                        f"Q{i}?\n".encode(), match_prefix(f"R{i}".encode()),
                        timeout=1.0)

    def echo(data: bytes) -> None:
        link.rx(f"R{data[1:2].decode()}VAL\n".encode())

    link.on_write = echo
    dev = DenonDevice(link.connect)
    notifications: list = []
    dev.subscribe(notifications.append)
    await dev.start()
    try:
        await asyncio.sleep(0)
        notifications.clear()

        await dev.query_state()
        await asyncio.sleep(0.01)
        # The whole burst coalesced to a single notification.
        assert len(notifications) == 1
        assert dev.state == {f"R{i}": "VAL" for i in range(burst)}
    finally:
        await dev.stop()


# ---------------------------------------------------------------- (e) ----

async def test_probe_idle_triggers_probe_and_answer_prevents_reconnect(
    link: FakeLink,
) -> None:
    """Idle link -> probe frame goes out; an answered probe (any RX) means
    the connection is alive: no reconnect."""

    class ProbedDevice(DictDevice):
        probe = ProbeSpec(frame=b"PING\n", idle=0.04, attempts=2)

    def echo(data: bytes) -> None:
        if data == b"PING\n":
            link.rx(b"PONG\n")

    link.on_write = echo
    dev = ProbedDevice(link.connect)
    await dev.start()
    try:
        await asyncio.sleep(0.2)  # several idle periods
        assert b"PING\n" in link.sent   # idle DID trigger probes
        assert link.connects == 1       # answered -> never declared dead
        assert dev.connected
    finally:
        await dev.stop()


async def test_probe_unanswered_after_attempts_triggers_reconnect(
    link: FakeLink,
) -> None:
    """A dead-silent device: after `attempts` unanswered probes the
    connection is declared lost and the reconnect loop runs."""

    class ProbedDevice(DictDevice):
        probe = ProbeSpec(frame=b"PING\n", idle=0.03, attempts=2)

    dev = ProbedDevice(link.connect)
    await dev.start()
    try:
        for _ in range(100):
            if link.connects >= 2:
                break
            await asyncio.sleep(0.01)
        assert link.connects >= 2  # probe failure -> reconnect happened
        # Exactly `attempts` probes were sent on the first connection.
        assert link.writers[0].written.count(b"PING\n") == 2
    finally:
        await dev.stop()


# ---------------------------------------------------------------- (f) ----

async def test_stop_rejects_in_flight_request_cleanly(link: FakeLink) -> None:
    """stop() is symmetric teardown: the in-flight request is failed (not
    left hanging), subscribers get None, and no reconnect follows."""
    dev = DictDevice(link.connect)
    snapshots: list = []
    dev.subscribe(snapshots.append)
    await dev.start()

    task = asyncio.ensure_future(
        dev.request(b"Q?\n", match_prefix(b"NEVER"), timeout=5.0))
    await asyncio.sleep(0.01)
    assert link.sent == [b"Q?\n"]

    await dev.stop()

    with pytest.raises(ConnectionLostError):
        await task
    assert not dev.connected
    assert link.writer.closed
    assert snapshots[-1] is None

    # No reconnect after stop(), ever.
    await asyncio.sleep(0.05)
    assert link.connects == 1

    # And post-stop requests fail fast instead of hanging.
    with pytest.raises(ConnectionLostError):
        await dev.request(b"Q?\n", match_prefix(b"X"), timeout=1.0)
