"""Byte-vector regression suites, one per framing style, using captures shaped
like the real device protocols. These lock the wire behaviour of the three
general framers against realistic input, including the anthem gen2 NUL-glue
vector.

The sony garbled-short-ack vector lives in the sony driver suite (Phase 3),
where the checksum-discriminated SonyFramer that owns that behaviour is
defined — the general framers here cannot express it (see the plan's known
dead-ends)."""

from __future__ import annotations

import pytest

from serialkit import (
    DelimiterFramer,
    LengthPrefixedFramer,
    RegexResyncFramer,
    ResyncError,
)


# ---- DelimiterFramer: anthem ';'-terminated, gen2 NUL-glue --------------

def test_anthem_semicolon_multiframe_vector() -> None:
    framer = DelimiterFramer(b";")
    wire = b"Z1POW1;Z1VOL-30;Z1MUT0;Z1SIM1;"
    assert framer.feed(wire) == [b"Z1POW1", b"Z1VOL-30", b"Z1MUT0", b"Z1SIM1"]


def test_gen2_nul_glue_wake_noise_vector() -> None:
    """Anthem gen2 emits NUL bytes glued around frames on wake; they are
    scrubbed even when a NUL splits a frame across read chunks, and doubled
    ';' delimiters yield no empty frames."""
    framer = DelimiterFramer(b";")
    # NULs lead, sit inside a frame, and trail; a chunk boundary splits Z1VOL.
    assert framer.feed(b"\x00\x00Z1POW1;;Z1VO") == [b"Z1POW1"]
    assert framer.feed(b"\x00L-30;\x00\x00Z1MUT0;\x00") == [
        b"Z1VOL-30",
        b"Z1MUT0",
    ]


def test_delimiter_oversize_desync_preserves_prior_frames() -> None:
    framer = DelimiterFramer(b";", max_frame=8)
    with pytest.raises(ResyncError) as excinfo:
        framer.feed(b"Z1POW1;" + b"\xff" * 40 + b";")
    assert excinfo.value.frames == (b"Z1POW1",)
    framer.reset()
    assert framer.feed(b"OK;") == [b"OK"]


# ---- RegexResyncFramer: LG 'x'-terminated with resync -------------------

# LG response: "[c2] [setid] (OK|NG)[data]x"; the terminator 'x' can also be
# the c2 byte (dx picture-mode / dy sound-mode), so we anchor on the whole
# structure instead of splitting on 'x'.
_LG = RegexResyncFramer  # alias for brevity in the vectors below
_LG_PATTERN = rb"([a-z] \d{2} (?:OK|NG)[0-9A-Fa-f]{2})x"


def test_lg_ok_response_vector() -> None:
    framer = _LG(_LG_PATTERN)
    assert framer.feed(b"a 01 OK01x") == [b"a 01 OK01"]
    # Two acks in one chunk.
    assert framer.feed(b"a 01 OK1ax b 01 NG00x") == [
        b"a 01 OK1a",
        b"b 01 NG00",
    ]


def test_lg_response_split_across_chunks() -> None:
    framer = _LG(_LG_PATTERN)
    assert framer.feed(b"a 01 OK") == []      # partial: no terminator yet
    assert framer.feed(b"01x") == [b"a 01 OK01"]


def test_lg_leading_garbage_is_resynced_away() -> None:
    framer = _LG(_LG_PATTERN)
    assert framer.feed(b"\x00\xffnoisea 01 NG00x") == [b"a 01 NG00"]


def test_lg_x_terminator_collision_with_c2() -> None:
    """dx (picture-mode) responses have c2='x', colliding with the 'x'
    terminator; anchoring on the full structure still frames them correctly."""
    framer = _LG(_LG_PATTERN)
    assert framer.feed(b"x 01 OK03x") == [b"x 01 OK03"]


def test_regex_oversize_residual_desyncs() -> None:
    framer = _LG(_LG_PATTERN, max_frame=16)
    with pytest.raises(ResyncError):
        framer.feed(b"z" * 40)  # never matches, residual overflows
    framer.reset()
    assert framer.feed(b"a 01 OK01x") == [b"a 01 OK01"]


# ---- LengthPrefixedFramer: 1-byte length header -------------------------

def test_length_prefixed_multiframe_vector() -> None:
    framer = LengthPrefixedFramer(1, lambda header: header[0])
    # header byte = payload length; the header is preserved in the frame.
    assert framer.feed(b"\x03ABC\x02DE") == [b"\x03ABC", b"\x02DE"]


def test_length_prefixed_split_across_chunks() -> None:
    framer = LengthPrefixedFramer(1, lambda header: header[0])
    assert framer.feed(b"\x04AB") == []          # header says 4, only 2 here
    assert framer.feed(b"CD\x01Z") == [b"\x04ABCD", b"\x01Z"]


def test_length_prefixed_oversize_desyncs() -> None:
    framer = LengthPrefixedFramer(1, lambda header: header[0], max_frame=8)
    with pytest.raises(ResyncError):
        framer.feed(b"\xff" + b"pad")  # declared 255 > max_frame 8
