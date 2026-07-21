"""serialkit.framing: split a byte stream into protocol frames.

A :class:`Framer` is a small stateful object owned by the runtime for the
lifetime of one connection. It buffers incoming bytes and yields complete
frames. On unrecoverable garbage it raises :class:`ResyncError` (carrying any
frames it managed to complete first); the runtime — never the framer — calls
:meth:`Framer.reset` to recover.

Three general framers are provided:

- :class:`DelimiterFramer` — frames terminated by a delimiter (anthem ``;``,
  newline-terminated ASCII protocols).
- :class:`RegexResyncFramer` — frames located by a regex, so leading garbage
  between matches is skipped (LG's ``x``-terminator, where the terminator can
  also appear inside a command).
- :class:`LengthPrefixedFramer` — a fixed-size header whose payload length is
  derived from the header bytes.

A protocol the general framers can't express (e.g. sony's checksum-
discriminated short-ack vs long frame) ships its own :class:`Framer`.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from .errors import ResyncError


@runtime_checkable
class Framer(Protocol):
    """Turns a byte stream into frames; one instance per connection."""

    def feed(self, data: bytes) -> list[bytes]:
        """Buffer ``data`` and return every complete frame now available.

        Raises :class:`ResyncError` (with ``frames=`` for anything completed
        before the desync) when the buffer is unrecoverable.
        """
        ...

    def reset(self) -> None:
        """Drop residual buffer (reconnect / desync recovery)."""
        ...


class DelimiterFramer:
    """Splits a byte stream into frames on a delimiter.

    - ``strip`` bytes (default NUL) are scrubbed from *incoming* data before
      buffering, so NUL glue between/inside frames never reaches a frame.
    - Residual (undelimited) data larger than ``max_frame``, or a single
      delimited frame larger than ``max_frame``, raises :class:`ResyncError`.
      The buffer is left intact; the caller must call :meth:`reset` to recover.
    """

    def __init__(
        self,
        delimiter: bytes,
        *,
        strip: bytes = b"\x00",
        max_frame: int = 4096,
    ) -> None:
        if not delimiter:
            raise ValueError("delimiter must be non-empty")
        self._delimiter = delimiter
        self._strip = strip
        self._max_frame = max_frame
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[bytes]:
        if self._strip:
            data = bytes(data).translate(None, delete=self._strip)
        self._buffer += data
        frames: list[bytes] = []
        while True:
            idx = self._buffer.find(self._delimiter)
            if idx < 0:
                break
            if idx > self._max_frame:
                raise ResyncError(
                    f"frame of {idx} bytes exceeds max_frame={self._max_frame}",
                    frames=frames,
                )
            frame = bytes(self._buffer[:idx])
            del self._buffer[: idx + len(self._delimiter)]
            if frame:  # skip empty frames (e.g. doubled delimiters)
                frames.append(frame)
        if len(self._buffer) > self._max_frame:
            raise ResyncError(
                f"residual of {len(self._buffer)} bytes exceeds "
                f"max_frame={self._max_frame} with no delimiter",
                frames=frames,
            )
        return frames

    def reset(self) -> None:
        self._buffer.clear()


class RegexResyncFramer:
    """Locates complete frames with a regex, skipping garbage between matches.

    The pattern must match a whole frame; ``feed`` returns the captured group
    (group 1 if the pattern has one, else the whole match) for each match, and
    discards everything up to the end of the last match. Bytes before the
    first match are dropped — this is the resync behaviour (LG's read loop
    finds ``...x``-terminated responses even when framing noise precedes them).

    Residual larger than ``max_frame`` with no match raises
    :class:`ResyncError`.
    """

    def __init__(
        self,
        pattern: bytes | re.Pattern[bytes],
        *,
        strip: bytes = b"",
        max_frame: int = 4096,
    ) -> None:
        self._pattern = re.compile(pattern) if isinstance(pattern, bytes) else pattern
        self._strip = strip
        self._max_frame = max_frame
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[bytes]:
        if self._strip:
            data = bytes(data).translate(None, delete=self._strip)
        self._buffer += data
        frames: list[bytes] = []
        end = 0
        for match in self._pattern.finditer(bytes(self._buffer)):
            frames.append(match.group(1) if match.groups() else match.group(0))
            end = match.end()
        if end:
            del self._buffer[:end]
        if len(self._buffer) > self._max_frame:
            raise ResyncError(
                f"residual of {len(self._buffer)} bytes exceeds "
                f"max_frame={self._max_frame} with no frame match",
                frames=frames,
            )
        return frames

    def reset(self) -> None:
        self._buffer.clear()


class LengthPrefixedFramer:
    """Frames whose payload length is encoded in a fixed-size header.

    The frame on the wire is ``header + payload``; ``length_of(header)``
    returns the payload byte count. ``feed`` yields ``header + payload`` for
    each complete frame (the header is preserved — most protocols checksum or
    address on it). A declared payload length larger than ``max_frame`` raises
    :class:`ResyncError`.
    """

    def __init__(
        self,
        header_size: int,
        length_of: "Callable[[bytes], int]",
        *,
        max_frame: int = 4096,
    ) -> None:
        if header_size < 1:
            raise ValueError("header_size must be >= 1")
        self._header_size = header_size
        self._length_of = length_of
        self._max_frame = max_frame
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[bytes]:
        self._buffer += data
        frames: list[bytes] = []
        while len(self._buffer) >= self._header_size:
            header = bytes(self._buffer[: self._header_size])
            payload_len = self._length_of(header)
            if payload_len > self._max_frame:
                raise ResyncError(
                    f"declared payload of {payload_len} bytes exceeds "
                    f"max_frame={self._max_frame}",
                    frames=frames,
                )
            total = self._header_size + payload_len
            if len(self._buffer) < total:
                break
            frames.append(bytes(self._buffer[:total]))
            del self._buffer[:total]
        return frames

    def reset(self) -> None:
        self._buffer.clear()
