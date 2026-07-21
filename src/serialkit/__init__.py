"""serialkit: an asyncio robustness toolkit for RS232 device drivers.

OTP-inspired single-dispatch-task internals (framing, request/response
correlation, pacing, reconnect, an opt-in liveness watchdog) behind an
``asyncio.Protocol``-flavoured callback API. Subclass :class:`SerialDevice`,
declare config as class attributes, and override the lifecycle callbacks; or
drop to the primitives (:class:`PendingTracker`, the framers, :class:`Pacing`)
when a protocol needs bespoke handling.
"""

from __future__ import annotations

from .correlate import Matcher, PendingTracker, match_predicate, match_prefix
from .device import Backoff, ProbeSpec, SerialDevice
from .errors import (
    CommandTimeoutError,
    ConnectionLostError,
    ProtocolError,
    ResyncError,
    SerialKitError,
)
from .framing import (
    DelimiterFramer,
    Framer,
    LengthPrefixedFramer,
    RegexResyncFramer,
)
from .pacing import Pacing

__all__ = [
    "Backoff",
    "CommandTimeoutError",
    "ConnectionLostError",
    "DelimiterFramer",
    "Framer",
    "LengthPrefixedFramer",
    "Matcher",
    "Pacing",
    "PendingTracker",
    "ProbeSpec",
    "ProtocolError",
    "RegexResyncFramer",
    "ResyncError",
    "SerialDevice",
    "SerialKitError",
    "match_predicate",
    "match_prefix",
]
