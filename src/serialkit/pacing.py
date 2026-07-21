"""serialkit.pacing: centrally enforced minimum spacing between sends."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager


class Pacing:
    """Minimum spacing between sends, enforced in a locked send path.

    Interval semantics (settle-after): the interval selected for a frame is
    the minimum delay *after that frame is sent* before the next send may go
    out (a power-on command needs settle time after it). Selection order:
    per-send ``pace`` override > longest matching ``per_command`` prefix >
    ``min_interval``.

    A chained command (e.g. ``b"PW?;MV?"``) sent as one frame passes through
    :meth:`send_slot` once and is therefore one pacing unit — it inherits the
    interval of whichever prefix it starts with.

    ``time_func``/``sleep_func`` are injectable for deterministic tests.
    """

    def __init__(
        self,
        min_interval: float = 0.0,
        per_command: Mapping[bytes, float] | None = None,
        *,
        time_func: Callable[[], float] | None = None,
        sleep_func: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._min_interval = min_interval
        self._per_command = dict(per_command or {})
        self._time_func = time_func
        self._sleep_func = sleep_func
        self._sleep = sleep_func or asyncio.sleep
        self._lock = asyncio.Lock()
        self._next_allowed = float("-inf")

    def clone(self) -> Pacing:
        """A fresh Pacing with the same config but its own lock and clock.

        Used by :class:`SerialDevice` so a ``Pacing`` declared as a class
        attribute is never shared (its lock and next-allowed timestamp) across
        instances or event loops.
        """
        return Pacing(
            self._min_interval,
            self._per_command,
            time_func=self._time_func,
            sleep_func=self._sleep_func,
        )

    def interval_for(self, frame: bytes, *, pace: float | None = None) -> float:
        """Interval to hold after ``frame`` before the next send."""
        if pace is not None:
            return pace
        best: float | None = None
        best_len = -1
        for prefix, interval in self._per_command.items():
            if frame.startswith(prefix) and len(prefix) > best_len:
                best, best_len = interval, len(prefix)
        return self._min_interval if best is None else best

    @asynccontextmanager
    async def send_slot(
        self, frame: bytes, *, pace: float | None = None
    ) -> AsyncIterator[None]:
        """Locked send path: delay until pacing allows, the caller writes
        inside the ``with`` body, then the frame's settle interval is recorded
        for the next sender.

        The settle interval is recorded whether or not the caller actually
        wrote (an abandoned paced write still reserves the slot — conservative
        and harmless).
        """
        async with self._lock:
            now = self._now()
            if self._next_allowed > now:
                await self._sleep(self._next_allowed - now)
            try:
                yield
            finally:
                self._next_allowed = self._now() + self.interval_for(
                    frame, pace=pace
                )

    def _now(self) -> float:
        if self._time_func is not None:
            return self._time_func()
        return asyncio.get_running_loop().time()
