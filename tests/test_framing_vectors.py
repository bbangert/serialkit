"""Byte-vector regression suites, one per framing style.

These lock the wire behaviour of the two general framers against input shaped
like real traffic rather than tidy examples: NUL glue around frames, doubled
delimiters, a terminator byte that also occurs inside a frame, and garbage
preceding a good frame.

A protocol neither general framer can express ships its own ``Framer``, and its
vectors live in that driver's suite."""

from __future__ import annotations

import pytest

from serialkit import (
    DelimiterFramer,
    RegexResyncFramer,
    ResyncError,
)

# ---- DelimiterFramer: ';'-terminated, with NUL glue ---------------------


def test_semicolon_multiframe_vector() -> None:
    framer = DelimiterFramer(b";")
    wire = b"POW1;VOL-30;MUT0;SRC1;"
    assert framer.feed(wire) == [b"POW1", b"VOL-30", b"MUT0", b"SRC1"]


def test_nul_glue_wake_noise_vector() -> None:
    """Some devices emit NUL bytes glued around frames while waking. They are
    scrubbed even when a NUL splits a frame across read chunks, and doubled
    ';' delimiters yield no empty frames."""
    framer = DelimiterFramer(b";")
    # NULs lead, sit inside a frame, and trail; a chunk boundary splits VOL.
    assert framer.feed(b"\x00\x00POW1;;VO") == [b"POW1"]
    assert framer.feed(b"\x00L-30;\x00\x00MUT0;\x00") == [
        b"VOL-30",
        b"MUT0",
    ]


def test_delimiter_oversize_desync_preserves_prior_frames() -> None:
    framer = DelimiterFramer(b";", max_frame=8)
    with pytest.raises(ResyncError) as excinfo:
        framer.feed(b"POW1;" + b"\xff" * 40 + b";")
    assert excinfo.value.frames == (b"POW1",)
    framer.reset()
    assert framer.feed(b"OK;") == [b"OK"]


# ---- RegexResyncFramer: 'x'-terminated with resync ----------------------

# A response shaped "[cmd] [id] (OK|NG)[data]x". The terminator 'x' can also
# occur as the command byte, so splitting on it would cut frames in half; the
# pattern anchors on the whole structure instead.
_Framer = RegexResyncFramer  # alias for brevity in the vectors below
_PATTERN = rb"([a-z] \d{2} (?:OK|NG)[0-9A-Fa-f]{2})x"


def test_ok_response_vector() -> None:
    framer = _Framer(_PATTERN)
    assert framer.feed(b"a 01 OK01x") == [b"a 01 OK01"]
    # Two acks in one chunk.
    assert framer.feed(b"a 01 OK1ax b 01 NG00x") == [
        b"a 01 OK1a",
        b"b 01 NG00",
    ]


def test_response_split_across_chunks() -> None:
    framer = _Framer(_PATTERN)
    assert framer.feed(b"a 01 OK") == []  # partial: no terminator yet
    assert framer.feed(b"01x") == [b"a 01 OK01"]


def test_leading_garbage_is_resynced_away() -> None:
    framer = _Framer(_PATTERN)
    assert framer.feed(b"\x00\xffnoisea 01 NG00x") == [b"a 01 NG00"]


def test_terminator_collides_with_command_byte() -> None:
    """A response whose command byte is itself 'x' collides with the
    terminator; anchoring on the full structure still frames it correctly."""
    framer = _Framer(_PATTERN)
    assert framer.feed(b"x 01 OK03x") == [b"x 01 OK03"]


def test_regex_oversize_residual_desyncs() -> None:
    framer = _Framer(_PATTERN, max_frame=16)
    with pytest.raises(ResyncError):
        framer.feed(b"z" * 40)  # never matches, residual overflows
    framer.reset()
    assert framer.feed(b"a 01 OK01x") == [b"a 01 OK01"]
