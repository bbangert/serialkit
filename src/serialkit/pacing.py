"""serialkit.pacing: how long the wire must settle after a frame is sent."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Pacing:
    """Minimum spacing between sends, as pure config.

    Interval semantics (settle-after): the interval selected for a frame is
    the minimum delay *after that frame is sent* before the next send may go
    out (a power-on command needs settle time after it). Selection order:
    per-send ``pace`` override > longest matching ``per_command`` prefix >
    ``min_interval``.

    A chained command (e.g. ``b"PW?;MV?"``) written as one frame passes the
    send path once and is therefore one pacing unit — it inherits the interval
    of whichever prefix it starts with.

    This object holds no lock and no clock: :class:`~serialkit.SerialLink` owns
    the send lock and the next-allowed timestamp. A ``Pacing`` is therefore
    safe to share between links or declare as a module-level constant.
    """

    min_interval: float = 0.0
    per_command: Mapping[bytes, float] = field(default_factory=dict)

    def interval_for(self, frame: bytes, *, pace: float | None = None) -> float:
        """Interval to hold after ``frame`` before the next send."""
        if pace is not None:
            return pace
        best: float | None = None
        best_len = -1
        for prefix, interval in self.per_command.items():
            if frame.startswith(prefix) and len(prefix) > best_len:
                best, best_len = interval, len(prefix)
        return self.min_interval if best is None else best
