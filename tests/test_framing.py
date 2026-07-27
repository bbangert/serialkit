"""DelimiterFramer regression scenarios."""

from __future__ import annotations

import pytest

from serialkit import DelimiterFramer, ResyncError


def test_split_frames_across_feeds() -> None:
    framer = DelimiterFramer(b";")
    assert framer.feed(b"PO") == []  # partial: held as residual
    assert framer.feed(b"W1;V") == [b"POW1"]
    assert framer.feed(b"OL-30;") == [b"VOL-30"]


def test_multiple_frames_in_one_feed() -> None:
    framer = DelimiterFramer(b";")
    assert framer.feed(b"POW1;VOL-30;;") == [b"POW1", b"VOL-30"]


def test_nul_glue_scrub() -> None:
    """NUL bytes glued into the stream — some devices emit them while waking
    — are scrubbed even when they split a frame across feeds."""
    framer = DelimiterFramer(b";")
    assert framer.feed(b"\x00\x00PO") == []
    assert framer.feed(b"\x00W1;") == [b"POW1"]
    assert framer.feed(b"\x00VOL-30;\x00") == [b"VOL-30"]


def test_oversize_residual_raises_resync_and_reset_recovers() -> None:
    framer = DelimiterFramer(b";", max_frame=16)
    with pytest.raises(ResyncError):
        framer.feed(b"\xff" * 32)  # garbage, no delimiter
    # Un-reset framer still holds the poisoned buffer.
    framer.reset()
    assert framer.feed(b"POW1;") == [b"POW1"]


def test_oversize_frame_raises_but_earlier_frames_survive() -> None:
    framer = DelimiterFramer(b";", max_frame=8)
    with pytest.raises(ResyncError) as excinfo:
        framer.feed(b"GOOD;" + b"X" * 20 + b";")
    assert excinfo.value.frames == (b"GOOD",)  # completed before desync
    framer.reset()
    assert framer.feed(b"OK;") == [b"OK"]


def test_reset_drops_residual() -> None:
    framer = DelimiterFramer(b";")
    framer.feed(b"partial-frame")
    framer.reset()
    assert framer.feed(b"NEW;") == [b"NEW"]
