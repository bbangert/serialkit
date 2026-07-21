"""Teardown: fail_all rejects everything, slots free up, no future is left
un-awaited (RuntimeWarnings are promoted to errors in pyproject.toml)."""

from __future__ import annotations

import asyncio

import pytest

from serialkit import (
    CommandTimeoutError,
    ConnectionLostError,
    PendingTracker,
    match_prefix,
)


async def test_fail_all_rejects_every_pending() -> None:
    tracker: PendingTracker = PendingTracker()
    futures = [
        await tracker.add(match_prefix(prefix), timeout=5.0)
        for prefix in (b"A", b"B", b"C")
    ]

    tracker.fail_all(ConnectionLostError("teardown"))

    results = await asyncio.gather(*futures, return_exceptions=True)
    assert all(isinstance(r, ConnectionLostError) for r in results)
    await asyncio.sleep(0)  # let done-callbacks unregister entries
    assert len(tracker) == 0
    # Nothing left to resolve.
    assert not tracker.feed(b"A1")


async def test_fail_all_releases_max_in_flight_slot() -> None:
    """A supervisor reconnect (fail_all) must not leave the single-flight
    gate permanently occupied."""
    tracker = PendingTracker(max_in_flight=1)
    fut = await tracker.add(match_prefix(b"A"), timeout=5.0)
    tracker.fail_all(ConnectionLostError("reconnect"))
    with pytest.raises(ConnectionLostError):
        await fut

    # The slot is free again: a new add() completes without blocking.
    fut2 = await asyncio.wait_for(
        tracker.add(match_prefix(b"B"), timeout=5.0), timeout=0.1
    )
    assert tracker.feed(b"B1")
    assert await fut2 == b"B1"


async def test_timeout_releases_slot() -> None:
    """A timed-out request must not leave its max_in_flight slot occupied."""
    tracker = PendingTracker(max_in_flight=1)
    fut = await tracker.add(match_prefix(b"A"), timeout=0.02)
    with pytest.raises(CommandTimeoutError):
        await fut
    fut2 = await asyncio.wait_for(
        tracker.add(match_prefix(b"B"), timeout=5.0), timeout=0.1
    )
    fut2.cancel()
    await asyncio.sleep(0)


async def test_caller_cancellation_releases_slot_and_unregisters() -> None:
    tracker = PendingTracker(max_in_flight=1)
    fut = await tracker.add(match_prefix(b"A"), timeout=5.0)
    fut.cancel()
    await asyncio.sleep(0)
    assert len(tracker) == 0
    fut2 = await asyncio.wait_for(
        tracker.add(match_prefix(b"B"), timeout=5.0), timeout=0.1
    )
    fut2.cancel()
    await asyncio.sleep(0)
