"""Pacing interval selection.

``Pacing`` is pure config: it answers "how long must the wire settle after
this frame". The timing behaviour built on those answers (the send lock, the
next-allowed timestamp, the actual sleep) lives in ``SerialLink`` and is
covered by ``test_link_pacing.py``.
"""

from __future__ import annotations

import dataclasses

import pytest

from serialkit import Pacing


def test_default_pacing_is_no_delay() -> None:
    assert Pacing().interval_for(b"MV50") == 0.0


def test_min_interval_applies_to_any_frame() -> None:
    pacing = Pacing(min_interval=0.1)
    assert pacing.interval_for(b"MV50") == pytest.approx(0.1)
    assert pacing.interval_for(b"PWON") == pytest.approx(0.1)


def test_per_command_longest_prefix_wins() -> None:
    pacing = Pacing(min_interval=0.1, per_command={b"PW": 0.5, b"PWON": 1.0})
    assert pacing.interval_for(b"PWON") == pytest.approx(1.0)
    assert pacing.interval_for(b"PWSTANDBY") == pytest.approx(0.5)
    assert pacing.interval_for(b"MV50") == pytest.approx(0.1)


def test_per_send_override_beats_per_command() -> None:
    pacing = Pacing(min_interval=0.1, per_command={b"PW": 0.5})
    assert pacing.interval_for(b"PWON", pace=2.0) == pytest.approx(2.0)
    assert pacing.interval_for(b"PWON", pace=0.0) == 0.0


def test_chained_command_inherits_its_leading_prefix() -> None:
    """A ;-chained write is ONE frame, so it selects one interval — the one
    for the prefix it starts with, not the sum of its subcommands."""
    pacing = Pacing(min_interval=0.1, per_command={b"POW": 1.0})
    assert pacing.interval_for(b"POW?;VOL?;MUT?") == pytest.approx(1.0)
    assert pacing.interval_for(b"VOL?;POW?") == pytest.approx(0.1)


def test_pacing_is_frozen_and_shareable() -> None:
    """Frozen and lock-free, so a module-level Pacing constant is safe to
    share between links."""
    pacing = Pacing(min_interval=0.1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        pacing.min_interval = 0.2  # type: ignore[misc]
    assert Pacing(min_interval=0.1) == pacing  # value semantics
