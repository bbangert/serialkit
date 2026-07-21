"""Pacing regression scenarios (deterministic via FakeClock)."""

from __future__ import annotations

import pytest

from serialkit import Pacing

from conftest import FakeClock


def make_pacing(clock: FakeClock, **kwargs: object) -> Pacing:
    return Pacing(time_func=clock.time, sleep_func=clock.sleep, **kwargs)


async def send(
    pacing: Pacing, clock: FakeClock, frame: bytes, pace: float | None = None
) -> float:
    """Send through the locked path; returns the time the write happened."""
    async with pacing.send_slot(frame, pace=pace):
        return clock.time()


async def test_three_rapid_sends_respect_min_interval(clock: FakeClock) -> None:
    pacing = make_pacing(clock, min_interval=0.1)
    t1 = await send(pacing, clock, b"MV50")
    t2 = await send(pacing, clock, b"MV51")
    t3 = await send(pacing, clock, b"MV52")
    assert t1 == 0.0
    assert t2 == pytest.approx(0.1)
    assert t3 == pytest.approx(0.2)


async def test_per_command_longest_prefix_wins(clock: FakeClock) -> None:
    pacing = make_pacing(
        clock, min_interval=0.1, per_command={b"PW": 0.5, b"PWON": 1.0}
    )
    t1 = await send(pacing, clock, b"PWON")   # longest prefix -> 1.0 settle
    t2 = await send(pacing, clock, b"PWSTANDBY")  # only b"PW" matches -> 0.5
    t3 = await send(pacing, clock, b"MV50")   # no prefix -> min_interval
    t4 = await send(pacing, clock, b"MV51")
    assert t2 - t1 == pytest.approx(1.0)
    assert t3 - t2 == pytest.approx(0.5)
    assert t4 - t3 == pytest.approx(0.1)


async def test_per_send_override_beats_per_command(clock: FakeClock) -> None:
    pacing = make_pacing(clock, min_interval=0.1, per_command={b"PW": 0.5})
    t1 = await send(pacing, clock, b"PWON", pace=2.0)
    t2 = await send(pacing, clock, b"MV50")
    assert t2 - t1 == pytest.approx(2.0)


async def test_chained_command_is_one_pacing_unit(clock: FakeClock) -> None:
    """A ;-chained write goes through the send path once: two chained
    frames are spaced by ONE interval, not one per subcommand."""
    pacing = make_pacing(clock, min_interval=0.1)
    t1 = await send(pacing, clock, b"Z1POW?;Z1VOL?;Z1MUT?")
    t2 = await send(pacing, clock, b"Z1SIM?;Z1AIC?")
    assert t2 - t1 == pytest.approx(0.1)
    assert clock.sleeps == [pytest.approx(0.1)]  # exactly one pacing delay
