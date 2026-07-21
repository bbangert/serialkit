"""Regression tests for the pinned contract semantics (Cycle-4 corrections and
the invented runtime semantics) that the ported suite doesn't already cover:
start()-failure propagation, not-connected fast-fail, runtime-owned framer
reset + ResyncError.frames routing, no-stale-snapshot-after-None, and the two
prototype-wart fixes (framer_factory ergonomics, per-instance Pacing)."""

from __future__ import annotations

import asyncio

import pytest

from serialkit import (
    ConnectionLostError,
    DelimiterFramer,
    Pacing,
    match_prefix,
)

from conftest import DictDevice, FakeLink


# ---- start() failure propagation (invented semantics #1) ----------------

async def test_start_propagates_connect_failure_and_starts_no_reconnect(
) -> None:
    async def failing_connect() -> tuple[object, object]:
        raise OSError("no such port")

    dev = DictDevice(failing_connect)
    with pytest.raises(OSError, match="no such port"):
        await dev.start()
    assert not dev.connected
    assert dev._monitor_task is None  # the reconnect loop never started


async def test_start_propagates_on_connect_failure_after_teardown(
    link: FakeLink,
) -> None:
    class BadHandshake(DictDevice):
        async def on_connect(self) -> None:
            raise RuntimeError("handshake rejected")

    dev = BadHandshake(link.connect)
    snapshots: list = []
    dev.subscribe(snapshots.append)

    with pytest.raises(RuntimeError, match="handshake rejected"):
        await dev.start()

    assert not dev.connected
    assert dev._monitor_task is None
    assert snapshots == [None]  # symmetric teardown delivered None
    # A failed start() supervises nothing: no reconnect attempt follows.
    await asyncio.sleep(0.05)
    assert link.connects == 1


# ---- not-connected fast-fail (invented semantics #6) --------------------

async def test_request_and_send_fail_fast_before_start(link: FakeLink) -> None:
    dev = DictDevice(link.connect)
    with pytest.raises(ConnectionLostError):
        await dev.request(b"X\n", match_prefix(b"Y"), timeout=1.0)
    with pytest.raises(ConnectionLostError):
        await dev.send(b"X\n")


# ---- runtime owns framer reset; ResyncError.frames are routed -----------

async def test_runtime_resets_framer_and_routes_pre_desync_frames(
    link: FakeLink,
) -> None:
    class SmallFrameDevice(DictDevice):
        framer_factory = staticmethod(lambda: DelimiterFramer(b"\n", max_frame=8))

    dev = SmallFrameDevice(link.connect)
    await dev.start()
    try:
        fut = await dev.pending.add(match_prefix(b"GOOD"), timeout=1.0)
        # GOOD1 completes, then 20 undelimited bytes overflow -> ResyncError
        # whose .frames still carries GOOD1.
        link.rx(b"GOOD1\n" + b"X" * 20)

        assert await fut == b"GOOD1"          # pre-desync frame was routed
        await asyncio.sleep(0.01)
        assert dev.frame_errors
        assert dev.frame_errors[-1][0] == b""  # framer desync recorded
        assert dev.connected                   # dispatch survived; no reconnect
        assert link.connects == 1

        # The runtime reset the framer, so the next frame parses cleanly.
        fut2 = await dev.pending.add(match_prefix(b"NEXT"), timeout=1.0)
        link.rx(b"NEXT1\n")
        assert await fut2 == b"NEXT1"
    finally:
        await dev.stop()


# ---- no stale snapshot after notify(None) -------------------------------

async def test_no_snapshot_delivered_after_none(link: FakeLink) -> None:
    """A dirty-but-unflushed snapshot must never follow None: None is
    delivered exactly once and is the last thing a subscriber sees for the
    session."""
    dev = DictDevice(link.connect)
    snapshots: list = []
    dev.subscribe(snapshots.append)
    await dev.start()
    await asyncio.sleep(0)  # flush the initial {} snapshot
    snapshots.clear()

    # Mark state dirty, then tear the session down before the flush can run.
    dev.state["pending"] = 1
    dev.notify()
    await dev.stop()
    await asyncio.sleep(0)  # let any scheduled flush run (it must be a no-op)

    assert snapshots[-1] is None
    assert None not in snapshots[:-1]  # None appears once, at the very end


# ---- prototype-wart fixes (P2-T2) ---------------------------------------

async def test_framer_factory_accepts_a_framer_instance_prototype(
    link: FakeLink,
) -> None:
    class ProtoDevice(DictDevice):
        framer_factory = DelimiterFramer(b"\n")  # instance, not a callable

    dev = ProtoDevice(link.connect)
    await dev.start()
    try:
        # The runtime copied the prototype rather than sharing it.
        assert dev._framer is not ProtoDevice.framer_factory
        assert isinstance(dev._framer, DelimiterFramer)

        fut = await dev.pending.add(match_prefix(b"A"), timeout=1.0)
        link.rx(b"A1\n")
        assert await fut == b"A1"
    finally:
        await dev.stop()


def test_pacing_class_attr_is_not_shared_across_instances() -> None:
    class PacedDevice(DictDevice):
        pacing = Pacing(min_interval=0.05)

    d1 = PacedDevice(FakeLink().connect)
    d2 = PacedDevice(FakeLink().connect)
    assert d1._pacing is not d2._pacing         # each instance gets its own
    assert d1._pacing is not PacedDevice.pacing  # not the shared class attr
