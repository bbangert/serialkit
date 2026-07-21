"""serialkit error hierarchy.

Device drivers subclass these (typically :class:`ProtocolError`) so a Home
Assistant coordinator can catch kit-level types without importing every
driver's private exceptions.
"""

from __future__ import annotations

from collections.abc import Iterable


class SerialKitError(Exception):
    """Base for all serialkit errors."""


class ConnectionLostError(SerialKitError):
    """The serial connection dropped (EOF, write failure, probe timeout)."""


class CommandTimeoutError(SerialKitError):
    """A pending request did not receive a matching frame in time."""


class ProtocolError(SerialKitError):
    """The device reported an error; device libraries subclass this."""


class ResyncError(SerialKitError):
    """The framer lost sync (oversized/garbled input); caller must reset().

    ``frames`` carries any complete frames extracted from the same ``feed()``
    call before sync was lost, so they are not silently dropped when the
    dispatch loop resets the framer.
    """

    def __init__(self, message: str, *, frames: Iterable[bytes] = ()) -> None:
        super().__init__(message)
        self.frames: tuple[bytes, ...] = tuple(frames)
