"""serialkit.waiting: waiting for a frame, correctly anchored.

RS232 devices are overwhelmingly not request/reply. A command is
fire-and-forget and the device reports state on its own schedule, so an
arriving frame carries no reliable evidence of which command caused it. This
module therefore offers **observation** and **exclusivity**, never correlation:

- :class:`WaitRegistry` owns the RX sequence counter and the armed
  ``expect`` waiters. A waiter is armed at call time — *before* the send — so a
  reply arriving in the same read chunk as the write cannot be missed. It
  **observes**: a resolving frame is still delivered to ``on_frame``, because
  the frame that confirms a power-on is also the state report the driver must
  apply.
- :class:`Exchange` holds the wire exclusively for one send-and-read round, for
  devices whose replies are only decodable in the context of the outstanding
  command. It **consumes**: a claimed frame does not reach ``on_frame``.

Exchange replies are anchored by arrival order rather than content, because
arrival order is the only discriminator such a protocol has. ``send()`` records
the RX counter at the moment the frame goes on the wire — after pacing, not
before it — and ``next()`` accepts only a strictly greater index. A late reply
to a previous, timed-out exchange therefore lands *behind* the anchor and is
discarded rather than being read as this exchange's answer. This is the classic
serial desync, and a content matcher cannot express the fix: when replies carry
no identifier the two frames are byte-identical, and only arrival order
distinguishes them.

Frame routing order, per arriving frame:

1. an open exchange that is expecting a reply **claims** the frame;
2. otherwise every armed ``expect`` waiter whose predicate matches resolves;
3. ``on_frame(frame)`` runs regardless of step 2.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from types import TracebackType
from typing import Protocol

from .errors import CommandTimeoutError

_LOGGER = logging.getLogger(__name__)

Match = Callable[[bytes], bool]


class ResultSink(Protocol):
    """Where an exchange reports whether a command was answered, so the link
    can count consecutive failures for :class:`~serialkit.FailureCount`."""

    def __call__(self, *, timed_out: bool) -> None: ...


@dataclass
class _Waiter:
    match: Match
    future: asyncio.Future[bytes]
    timer: asyncio.TimerHandle | None = None

    def cancel_timer(self) -> None:
        if self.timer is not None:
            self.timer.cancel()
            self.timer = None


def _matches(match: Match, frame: bytes) -> bool:
    """A predicate that raises is treated as "no match", never as a crash.

    Predicates are driver code running on the dispatch task; one bad one must
    not take the read loop down with it.
    """
    try:
        return bool(match(frame))
    except Exception:
        _LOGGER.exception("expect() predicate raised; treating as no match")
        return False


class WaitRegistry:
    """The RX counter, the armed ``expect`` waiters, and the open exchange."""

    def __init__(self) -> None:
        self.rx_count = 0
        self._waiters: list[_Waiter] = []
        self._exchange: Exchange | None = None

    # ---- expect ---------------------------------------------------------

    def arm(self, match: Match, *, timeout: float) -> Awaitable[bytes]:
        """Register a waiter NOW and return its awaitable.

        The timeout clock starts here, at arm time, not at first await — the
        point of arming before the send is that the whole window is covered.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bytes] = loop.create_future()
        waiter = _Waiter(match, future)
        self._waiters.append(waiter)
        waiter.timer = loop.call_later(timeout, self._expire, waiter, timeout)
        future.add_done_callback(lambda _: waiter.cancel_timer())
        return future

    def _expire(self, waiter: _Waiter, timeout: float) -> None:
        self._discard(waiter)
        if not waiter.future.done():
            waiter.future.set_exception(
                CommandTimeoutError(f"no matching frame within {timeout}s")
            )

    def _discard(self, waiter: _Waiter) -> None:
        waiter.cancel_timer()
        with suppress(ValueError):
            self._waiters.remove(waiter)

    def cancel(self, awaitable: Awaitable[bytes]) -> None:
        """Drop an armed waiter whose send never made it out."""
        for waiter in list(self._waiters):
            if waiter.future is awaitable:
                self._discard(waiter)
                waiter.future.cancel()

    # ---- exchange -------------------------------------------------------

    def open_exchange(self, exchange: Exchange) -> None:
        self._exchange = exchange

    def close_exchange(self, exchange: Exchange) -> list[bytes]:
        """Close ``exchange`` and hand back any frames it claimed but never
        consumed, so they can be delivered normally instead of vanishing."""
        if self._exchange is not exchange:
            return []
        self._exchange = None
        return exchange.drain_unclaimed()

    # ---- routing --------------------------------------------------------

    def route(self, frame: bytes) -> bool:
        """Advance the RX counter and offer ``frame``.

        Returns ``True`` if an exchange claimed it (so it must NOT reach
        ``on_frame``), ``False`` otherwise. Armed ``expect`` waivers that match
        are resolved here but never consume the frame.
        """
        self.rx_count += 1
        exchange = self._exchange
        if exchange is not None and exchange.expecting:
            exchange.offer(self.rx_count, frame)
            return True
        for waiter in list(self._waiters):
            if not waiter.future.done() and _matches(waiter.match, frame):
                self._discard(waiter)
                waiter.future.set_result(frame)
        return False

    def fail_all(self, exc: Exception) -> None:
        """Fail every in-flight wait immediately (connection loss, or a
        driver-reported protocol error that makes waiting pointless)."""
        for waiter in list(self._waiters):
            self._discard(waiter)
            if not waiter.future.done():
                waiter.future.set_exception(exc)
        if self._exchange is not None:
            self._exchange.fail(exc)


class Exchange:
    """One exclusive send-and-read round on the wire.

    Obtained from ``async with link.exchange() as ex``. The link's wire lock is
    held for the whole block, so only one exchange runs at a time.
    """

    def __init__(
        self,
        registry: WaitRegistry,
        write: Callable[..., Awaitable[None]],
        on_result: ResultSink,
    ) -> None:
        self._registry = registry
        self._write = write
        self._on_result = on_result
        self._anchor: int | None = None
        self._queue: list[tuple[int, bytes]] = []
        self._waiter: asyncio.Future[bytes] | None = None

    @property
    def expecting(self) -> bool:
        """True once a send has gone out: the window in which arriving frames
        belong to this exchange. Before the first send nothing is outstanding,
        so frames route normally."""
        return self._anchor is not None

    async def send(self, frame: bytes, *, pace: float | None = None) -> None:
        """Pace, write, and anchor at the RX count *at write time*.

        Anchoring after the pacing delay rather than before it is what makes
        the anchor mean "frames that could possibly answer this": a reply to
        the previous command arriving while this one waits its pacing turn
        lands behind the anchor and is discarded.
        """
        await self._write(frame, pace=pace, before_write=self._anchor_here)

    def _anchor_here(self) -> None:
        self._anchor = self._registry.rx_count

    # ASYNC109: an explicit deadline, for the reason given in link.py's
    # module docstring — the driver knows how long this device takes.
    async def next(self, timeout: float) -> bytes:  # noqa: ASYNC109
        """The next frame that arrived strictly after the last :meth:`send`."""
        if self._anchor is None:
            raise RuntimeError("Exchange.next() called before send()")
        while self._queue:
            index, frame = self._queue.pop(0)
            if index > self._anchor:
                self._on_result(timed_out=False)
                return frame
            self._log_late(frame)
        loop = asyncio.get_running_loop()
        self._waiter = loop.create_future()
        try:
            frame = await asyncio.wait_for(self._waiter, timeout)
        except TimeoutError:
            self._on_result(timed_out=True)
            raise CommandTimeoutError(f"no reply within {timeout}s") from None
        finally:
            self._waiter = None
        self._on_result(timed_out=False)
        return frame

    def offer(self, index: int, frame: bytes) -> None:
        """Take a claimed frame: resolve a pending :meth:`next`, or queue it."""
        assert self._anchor is not None
        if index <= self._anchor:
            self._log_late(frame)
            return
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(frame)
        else:
            self._queue.append((index, frame))

    def _log_late(self, frame: bytes) -> None:
        _LOGGER.debug(
            "discarding frame %r that predates this exchange's send "
            "(late reply to a previous command)",
            frame,
        )

    def drain_unclaimed(self) -> list[bytes]:
        frames = [frame for _, frame in self._queue]
        self._queue.clear()
        return frames

    def fail(self, exc: Exception) -> None:
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_exception(exc)


@dataclass
class ExchangeContext:
    """Async context manager wrapping wire-lock acquisition around an
    :class:`Exchange`."""

    lock: asyncio.Lock
    registry: WaitRegistry
    write: Callable[..., Awaitable[None]]
    on_result: ResultSink
    release: Callable[[list[bytes]], None]
    _exchange: Exchange | None = field(default=None, init=False)

    async def __aenter__(self) -> Exchange:
        await self.lock.acquire()
        self._exchange = Exchange(self.registry, self.write, self.on_result)
        self.registry.open_exchange(self._exchange)
        return self._exchange

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        assert self._exchange is not None
        try:
            unclaimed = self.registry.close_exchange(self._exchange)
        finally:
            self.lock.release()
        if unclaimed:
            self.release(unclaimed)


__all__ = ["Exchange", "ExchangeContext", "WaitRegistry"]
