"""Sony desync regression, through the SerialDevice runtime: answer frames
echo nothing about the request, so matchers only validate shape. With
max_in_flight=1 a garbled/dropped answer plus timeout must NOT shift
correlation onto the next command. The contrast test shows the ungated
FIFO-degenerate behavior misattributing."""

from __future__ import annotations

import asyncio

import pytest

from serialkit import CommandTimeoutError, Pacing

from conftest import DictDevice, FakeLink


def shape_matcher(frame: bytes) -> bool:
    """Sony-style: any well-formed answer frame matches (no correlation)."""
    return frame.startswith(b"ANS")


class GatedDevice(DictDevice):
    max_in_flight = 1


async def test_gated_tracker_does_not_shift_correlation(
    link: FakeLink,
) -> None:
    dev = GatedDevice(link.connect)
    await dev.start()
    try:
        task_a = asyncio.ensure_future(
            dev.request(b"CMD_A", shape_matcher, timeout=0.05))
        await asyncio.sleep(0.01)
        task_b = asyncio.ensure_future(
            dev.request(b"CMD_B", shape_matcher, timeout=1.0))
        await asyncio.sleep(0.01)

        # B is gated: its frame must not even be written while A's answer
        # is owed.
        assert link.sent == [b"CMD_A"]

        # A's answer was garbled on the wire (dropped by the framer):
        # nothing arrives, A times out.
        with pytest.raises(CommandTimeoutError):
            await task_a

        # Only after A's future completed does B get the slot and the wire.
        await asyncio.sleep(0.01)
        assert link.sent == [b"CMD_A", b"CMD_B"]

        # The next answer on the wire belongs to B and resolves B.
        link.rx(b"ANS_B\n")
        assert await task_b == b"ANS_B"
        assert len(dev.pending) == 0
    finally:
        await dev.stop()


async def test_contrast_ungated_tracker_misattributes(link: FakeLink) -> None:
    """Without the gate, shape-only matchers degenerate to FIFO: B's answer
    resolves A. This is today's sony bug, demonstrated on purpose."""
    dev = DictDevice(link.connect)  # no max_in_flight
    await dev.start()
    try:
        task_a = asyncio.ensure_future(
            dev.request(b"CMD_A", shape_matcher, timeout=0.2))
        task_b = asyncio.ensure_future(
            dev.request(b"CMD_B", shape_matcher, timeout=0.2))
        await asyncio.sleep(0.01)
        assert link.sent == [b"CMD_A", b"CMD_B"]  # both hit the wire

        # A's answer was dropped; the frame that arrives is B's answer...
        link.rx(b"ANS_B\n")

        # ...but oldest-matching-first hands it to A: misattribution.
        assert await task_a == b"ANS_B"
        with pytest.raises(CommandTimeoutError):
            await task_b
    finally:
        await dev.stop()


async def test_timeout_during_paced_write_abandons_the_write(
    link: FakeLink,
) -> None:
    """Pinned contract: the timeout timer starts in pending.add() (slot
    acquisition), BEFORE pacing releases the write. If the timeout fires
    while the frame is still queued behind pacing, request() must abandon
    the write — otherwise an untracked command goes on the wire after its
    slot was already released, and the device's answer to it desyncs the
    next request (the exact sony failure the gate prevents)."""

    class PacedGatedDevice(DictDevice):
        max_in_flight = 1
        pacing = Pacing(min_interval=0.08)

    dev = PacedGatedDevice(link.connect)
    await dev.start()
    try:
        await dev.send(b"PRIME")  # pacing now holds the wire for 80ms

        # Timeout (20ms) fires while CMD is still waiting on pacing (80ms).
        with pytest.raises(CommandTimeoutError):
            await dev.request(b"CMD", shape_matcher, timeout=0.02)

        await asyncio.sleep(0.1)
        assert b"CMD" not in link.sent  # the stale write never hit the wire

        # And the link is still healthy: the next request correlates cleanly.
        task = asyncio.ensure_future(
            dev.request(b"NEXT", shape_matcher, timeout=1.0))
        await asyncio.sleep(0.01)
        link.rx(b"ANS_NEXT\n")
        assert await task == b"ANS_NEXT"
    finally:
        await dev.stop()
