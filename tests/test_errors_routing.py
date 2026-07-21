"""Gen1/gen2 error-frame regression scenarios + driver-ordered on_frame
routing. There is no FrameRouter/reducer/error_classifier — the scenarios are
expressed as driver code in a sync on_frame callback running on the
SerialDevice dispatch task."""

from __future__ import annotations

import asyncio

import pytest

from serialkit import (
    DelimiterFramer,
    PendingTracker,
    ProtocolError,
    match_prefix,
)

from conftest import DictDevice, FakeLink


class SemiDevice(DictDevice):
    """DictDevice with anthem-style ';' framing."""

    framer_factory = staticmethod(lambda: DelimiterFramer(b";"))


async def test_gen1_uncorrelatable_error_rejects_oldest_only() -> None:
    """Anthem gen1 error phrases carry no correlating content: reject the
    OLDEST pending only; a later pending still resolves normally."""
    tracker: PendingTracker = PendingTracker()
    fut_volume = await tracker.add(match_prefix(b"P1VM"), timeout=1.0)
    fut_source = await tracker.add(match_prefix(b"P1S"), timeout=1.0)

    assert tracker.reject_oldest(ProtocolError("Command Error"))

    with pytest.raises(ProtocolError):
        await fut_volume
    assert not fut_source.done()

    assert tracker.feed(b"P1S3")
    assert await fut_source == b"P1S3"


async def test_gen2_echoed_error_rejects_matched_only(link: FakeLink) -> None:
    """Anthem gen2 errors echo the original command (!RZ1VOL+50): the driver
    strips the error prefix in on_frame and rejects the MATCHED pending; the
    other in-flight query survives and resolves."""

    class Gen2Device(SemiDevice):
        def on_frame(self, frame: bytes) -> None:
            if frame.startswith(b"!R"):
                # Strip the error prefix so matchers written against success
                # frames can correlate the rejection.
                self.pending.reject_matched(
                    frame[2:], ProtocolError(frame.decode()))
                return
            self.pending.feed(frame)

    dev = Gen2Device(link.connect)
    await dev.start()
    try:
        fut_set = await dev.pending.add(match_prefix(b"Z1VOL"), timeout=1.0)
        fut_query = await dev.pending.add(match_prefix(b"Z1POW"), timeout=1.0)

        link.rx(b"!RZ1VOL+50;")
        with pytest.raises(ProtocolError):
            await fut_set
        assert not fut_query.done()

        link.rx(b"Z1POW1;")
        assert await fut_query == b"Z1POW1"
    finally:
        await dev.stop()


async def test_reject_matched_with_no_match_is_noop() -> None:
    tracker: PendingTracker = PendingTracker()
    fut = await tracker.add(match_prefix(b"Z1POW"), timeout=1.0)
    assert not tracker.reject_matched(
        b"Z9VOL+50", ProtocolError("no owner"))
    assert not fut.done()
    fut.cancel()
    await asyncio.sleep(0)


async def test_reject_matched_all_rejects_every_match() -> None:
    """Gen2 rejects ALL pendings matching the echoed error, not just one."""
    tracker: PendingTracker = PendingTracker()
    fut_a = await tracker.add(match_prefix(b"Z1VOL"), timeout=1.0)
    fut_b = await tracker.add(match_prefix(b"Z1VOL"), timeout=1.0)
    fut_other = await tracker.add(match_prefix(b"Z1POW"), timeout=1.0)

    assert tracker.reject_matched(
        b"Z1VOL+50", ProtocolError("Z1VOL error"), all=True)

    with pytest.raises(ProtocolError):
        await fut_a
    with pytest.raises(ProtocolError):
        await fut_b
    assert not fut_other.done()
    fut_other.cancel()
    await asyncio.sleep(0)


async def test_reject_matched_single_rejects_only_oldest_match() -> None:
    """all=False (default) rejects only the oldest matching pending."""
    tracker: PendingTracker = PendingTracker()
    fut_a = await tracker.add(match_prefix(b"Z1VOL"), timeout=1.0)
    fut_b = await tracker.add(match_prefix(b"Z1VOL"), timeout=1.0)

    assert tracker.reject_matched(b"Z1VOL+50", ProtocolError("one only"))

    with pytest.raises(ProtocolError):
        await fut_a
    assert not fut_b.done()
    assert tracker.feed(b"Z1VOL+50")  # the survivor still resolves
    assert await fut_b == b"Z1VOL+50"


async def test_matcher_exception_is_treated_as_no_match() -> None:
    tracker: PendingTracker = PendingTracker()

    def bad_matcher(frame: bytes) -> bool:
        raise ValueError("driver bug")

    fut_bad = await tracker.add(bad_matcher, timeout=1.0)
    fut_ok = await tracker.add(match_prefix(b"OK"), timeout=1.0)

    assert tracker.feed(b"OK1")  # skips the raising matcher
    assert await fut_ok == b"OK1"
    assert not fut_bad.done()
    fut_bad.cancel()
    await asyncio.sleep(0)


async def test_on_frame_state_first_then_pending_still_resolves(
    link: FakeLink,
) -> None:
    """A frame that both updates state and answers a pending does BOTH:
    the driver's on_frame mutates state in place and notifies FIRST, then
    feeds the tracker (gen1/gen2/LG ordering, now plain driver code)."""

    class VolDevice(SemiDevice):
        def on_frame(self, frame: bytes) -> None:
            if frame.startswith(b"Z1VOL"):
                self.state["volume"] = int(frame[5:])
                self.notify()
            self.pending.feed(frame)

    dev = VolDevice(link.connect)
    snapshots: list = []
    dev.subscribe(snapshots.append)
    await dev.start()
    try:
        await asyncio.sleep(0)  # drain start()'s initial coalesced notify
        snapshots.clear()

        fut = await dev.pending.add(match_prefix(b"Z1VOL"), timeout=1.0)
        link.rx(b"Z1VOL-30;")

        assert await fut == b"Z1VOL-30"          # the future resolved
        assert dev.state["volume"] == -30        # AND state updated
        await asyncio.sleep(0)                   # let the coalesced flush run
        assert snapshots == [{"volume": -30}]    # AND subscribers notified
    finally:
        await dev.stop()


async def test_frame_exception_hardening_keeps_dispatch_alive(
    link: FakeLink,
) -> None:
    """An on_frame crash on one frame must not prevent the next frame in
    the same dispatch turn from routing, nor kill the dispatch task."""

    class CrashyDevice(SemiDevice):
        def on_frame(self, frame: bytes) -> None:
            if frame == b"BAD":
                raise RuntimeError("boom")
            self.pending.feed(frame)

    dev = CrashyDevice(link.connect)
    await dev.start()
    try:
        fut = await dev.pending.add(match_prefix(b"GOOD"), timeout=1.0)
        link.rx(b"BAD;GOOD1;")

        assert await fut == b"GOOD1"
        assert len(dev.frame_errors) == 1
        assert dev.frame_errors[0][0] == b"BAD"
        assert dev.connected            # dispatch survived the crash
        assert link.connects == 1       # and no reconnect was triggered
    finally:
        await dev.stop()
