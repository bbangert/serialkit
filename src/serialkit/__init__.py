"""serialkit: wire mechanics for RS232 device drivers.

Framing, pacing, exclusivity, sequence anchoring, dispatch, reconnect and
liveness — behind an ``asyncio.Protocol``-flavoured callback surface. The kit
knows nothing about any device: no commands, no responses, no state. A driver
implements :class:`DeviceHandler`, owns a :class:`SerialLink`, and keeps its
device model to itself.
"""

from __future__ import annotations

from .errors import (
    CommandTimeoutError,
    ConnectionLostError,
    ProtocolError,
    ResyncError,
    SerialKitError,
)
from .framing import DelimiterFramer, Framer, RegexResyncFramer
from .link import Backoff, DeviceHandler, FailureCount, IdleProbe, SerialLink
from .pacing import Pacing

__all__ = [
    "Backoff",
    "CommandTimeoutError",
    "ConnectionLostError",
    "DelimiterFramer",
    "DeviceHandler",
    "FailureCount",
    "Framer",
    "IdleProbe",
    "Pacing",
    "ProtocolError",
    "RegexResyncFramer",
    "ResyncError",
    "SerialKitError",
    "SerialLink",
]
