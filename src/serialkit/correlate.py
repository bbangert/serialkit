"""serialkit.correlate: correlate response frames to in-flight requests.

Correlation is matcher-based, never positional. Positional (FIFO) correlation
is the exact defect that desyncs shape-only protocols like sony: a dropped or
garbled answer shifts every subsequent response by one. A :class:`PendingTracker`
with ``max_in_flight=1`` additionally refuses to even write a second command
while the first is owed a reply.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from .errors import CommandTimeoutError

Matcher = Callable[[bytes], bool]


def match_prefix(prefix: bytes) -> Matcher:
    """Match frames that start with ``prefix`` (the common case)."""

    def _match(frame: bytes) -> bool:
        return frame.startswith(prefix)

    return _match


def match_predicate(fn: Matcher) -> Matcher:
    """Identity wrapper documenting a hand-written matcher predicate."""
    return fn


class _Pending:
    __slots__ = ("matcher", "future", "timer")

    def __init__(self, matcher: Matcher, future: asyncio.Future[bytes]) -> None:
        self.matcher = matcher
        self.future = future
        self.timer: asyncio.TimerHandle | None = None

    def matches(self, frame: bytes) -> bool:
        """Run the matcher; a matcher that raises counts as no-match."""
        try:
            return bool(self.matcher(frame))
        except Exception:
            return False


class PendingTracker:
    """Tracks in-flight requests and correlates response frames to them.

    ``max_in_flight`` is a slot gate covering write AND response-wait: with
    ``max_in_flight=1``, a second :meth:`add` does not return until the first
    pending future completes (resolved, rejected, timed out, or failed) — so
    the caller cannot even write its frame while another response is owed.
    The timeout clock starts when the slot is acquired, so time spent queued
    for a slot does not consume the request's timeout budget.
    """

    def __init__(self, *, max_in_flight: int | None = None) -> None:
        self._pending: list[_Pending] = []
        self._slots = (
            asyncio.Semaphore(max_in_flight) if max_in_flight is not None else None
        )

    def __len__(self) -> int:
        return len(self._pending)

    async def add(
        self, matcher: Matcher, *, timeout: float
    ) -> asyncio.Future[bytes]:
        """Register a pending request; return the future for its response.

        Awaits a free slot first when ``max_in_flight`` is set. The slot is
        released via a future done-callback, so every completion path
        (resolve, reject, timeout, ``fail_all``, caller cancellation) frees it.
        """
        if self._slots is not None:
            await self._slots.acquire()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bytes] = loop.create_future()
        entry = _Pending(matcher, future)
        entry.timer = loop.call_later(timeout, self._on_timeout, entry)
        self._pending.append(entry)
        future.add_done_callback(lambda _fut: self._finalize(entry))
        return future

    def feed(self, frame: bytes) -> bool:
        """Resolve the oldest pending whose matcher accepts ``frame``.

        Returns ``False`` when no pending matches (the frame is unsolicited),
        so drivers can branch on it inside ``on_frame``.
        """
        for entry in self._pending:
            if not entry.future.done() and entry.matches(frame):
                entry.future.set_result(frame)
                return True
        return False

    def reject_matched(
        self, key: bytes, exc: Exception, *, all: bool = False
    ) -> bool:
        """Reject pending(s) whose matcher accepts ``key`` with ``exc``.

        ``key`` is the correlating content, which for an echoed error frame is
        classifier-derived (e.g. ``!RZ1VOL+50`` → ``Z1VOL+50``) so it matches
        the success matcher the request registered.

        - ``all=False`` (default): reject only the oldest matching pending
          (anthem gen1's single-owner errors).
        - ``all=True``: reject every matching pending (anthem gen2 rejects the
          whole matching set on an error reply).

        Returns ``True`` if at least one pending was rejected.
        """
        live = [e for e in self._pending if not e.future.done()]
        matched = [e for e in live if e.matches(key)]
        targets = matched if all else matched[:1]
        for entry in targets:
            entry.future.set_exception(exc)
        return bool(targets)

    def reject_oldest(self, exc: Exception) -> bool:
        """Reject the oldest live pending with ``exc``, ignoring matchers.

        For uncorrelatable error frames (anthem gen1 "Command Error" carries
        no echo of what failed). Returns ``True`` if a pending was rejected.
        """
        for entry in self._pending:
            if not entry.future.done():
                entry.future.set_exception(exc)
                return True
        return False

    def fail_all(self, exc: Exception) -> None:
        """Teardown/reconnect: reject every live pending with ``exc``."""
        for entry in list(self._pending):
            if not entry.future.done():
                entry.future.set_exception(exc)

    def _on_timeout(self, entry: _Pending) -> None:
        if not entry.future.done():
            entry.future.set_exception(
                CommandTimeoutError("no matching frame within timeout")
            )

    def _finalize(self, entry: _Pending) -> None:
        """Done-callback: unregister, cancel the timer, release the slot.

        Runs for every completion path, so a timed-out request never leaves
        its ``max_in_flight`` slot occupied. Done-callbacks fire via
        ``call_soon``, so unregistration is a tick later — :meth:`feed` skips
        already-done entries rather than relying on list removal.
        """
        if entry in self._pending:
            self._pending.remove(entry)
        if entry.timer is not None:
            entry.timer.cancel()
        if self._slots is not None:
            self._slots.release()
