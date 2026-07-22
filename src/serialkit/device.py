"""serialkit.device: the ``SerialDevice`` callback runtime.

OTP-inspired internals behind an ``asyncio.Protocol``-flavoured callback
surface. One dispatch task per connection reads the transport, frames each
chunk, and calls the sync ``on_frame`` callback per frame (exception-hardened),
then delivers at most one coalesced subscriber notification per dispatch turn.
A kit-owned reconnect loop rebuilds a fresh framer and fresh state
(``make_state``) on every connection.

Pinned semantics (see the README "callback contract" and the plan's research
notes):

- The request timeout clock starts at slot acquisition (``PendingTracker.add``),
  so it covers pacing + write + response wait, but not queue-wait for a slot.
- The paced write is ABANDONED if the pending future completed while the frame
  was queued behind pacing (timeout / fail_all / cancellation): the runtime
  never emits a frame whose pending is already gone (that is the sony desync).
- A request caller resumes strictly after the dispatch turn containing its
  response frame.
- ``notify(None)`` (disconnect) discards any dirty-but-unflushed snapshot, so
  subscribers can never see a stale snapshot after ``None``.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from .correlate import Matcher, PendingTracker
from .errors import ConnectionLostError, ResyncError
from .framing import Framer
from .pacing import Pacing

S = TypeVar("S")

_READ_CHUNK = 4096


@dataclass(frozen=True)
class Backoff:
    """Reconnect backoff policy: ``initial * factor**tries``, capped."""

    initial: float = 0.5
    factor: float = 1.8
    max_delay: float = 60.0

    def delay(self, tries: int) -> float:
        return min(self.initial * self.factor**tries, self.max_delay)


@dataclass(frozen=True)
class ProbeSpec:
    """Idle watchdog (any-RX-alive): after ``idle`` seconds with no RX, send
    ``frame``. ANY received data counts as liveness, not just a frame that
    answers the probe. After ``attempts`` consecutive unanswered probes the
    connection is declared dead and the reconnect loop runs.

    Liveness is judged by idle-window checkpoints (was there any RX during the
    window?), never by a ``now - last_rx`` clock delta — the latter is flaky
    under scheduling jitter and reconnects healthy, probe-answering devices.
    """

    frame: bytes
    idle: float
    attempts: int = 3


class SerialDevice(Generic[S]):
    """Callback-surface serial device runtime.

    Drivers subclass, declare config as class attributes, and override the
    lifecycle callbacks. The transport is injected as an async ``connect``
    factory returning a duck-typed ``(reader, writer)`` pair:
    ``reader.read(n) -> bytes`` (empty = EOF), ``writer.write(bytes)``,
    ``writer.close()``.

    ``framer_factory`` must be a ``staticmethod`` — a plain function assigned
    as a class attribute would become a bound method and receive ``self``.
    """

    # ---- driver-declared config (class attributes) ----
    # A fresh framer per connection. Either a zero-arg callable (a
    # ``staticmethod`` lambda, or any function — the runtime reads it off the
    # class, so it is never bound to ``self``) OR a Framer instance used as a
    # prototype (the runtime deep-copies and resets it per connection).
    framer_factory: Callable[[], Framer] | Framer
    # A shared mutable class-level Pacing (lock + next-allowed timestamp)
    # across all instances is a trap, so None means "kit creates a
    # per-instance Pacing()"; a subclass that sets a Pacing gets a private
    # clone per instance (see __init__). Backoff and ProbeSpec are frozen
    # dataclasses, so sharing them as class attributes is safe.
    pacing: Pacing | None = None
    probe: ProbeSpec | None = None  # None = no watchdog; explicit opt-in
    backoff: Backoff = Backoff()
    max_in_flight: int | None = None  # None = unlimited; sony sets 1
    request_timeout: float = 3.0

    def __init__(
        self, connect: Callable[[], Awaitable[tuple[Any, Any]]]
    ) -> None:
        self._connect = connect
        self.pending = PendingTracker(max_in_flight=self.max_in_flight)
        self._pacing = self.pacing.clone() if self.pacing is not None else Pacing()
        self.state: S | None = None
        self.connected = False
        # Per-frame hardening log: (frame, exc) for every on_frame crash and
        # (b"", ResyncError) for framer desyncs.
        self.frame_errors: list[tuple[bytes, Exception]] = []
        self._subscribers: list[Callable[[S | None], None]] = []
        self._reader: Any = None
        self._writer: Any = None
        self._framer: Framer | None = None
        self._dispatch_task: asyncio.Task[None] | None = None
        self._probe_task: asyncio.Task[None] | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        self._session_lost: asyncio.Future[Exception] | None = None
        self._stopped = False
        self._last_rx = 0.0
        self._notify_dirty = False
        self._notify_scheduled = False
        self._batch_depth = 0
        # Bumped on every connection. A request/send captures it before it
        # awaits a slot/pacing; if it changes underneath (a reconnect happened
        # while the caller was queued), the write is abandoned so a stale frame
        # can never land on a new session.
        self._session_id = 0

    # ---- lifecycle callbacks (driver overrides) ----

    def make_state(self) -> S:
        """Fresh state per CONNECTION (rebuild-on-reconnect)."""
        raise NotImplementedError

    async def on_connect(self) -> None:
        """Handshake/verify/initial query; frames are already flowing."""

    def on_frame(self, frame: bytes) -> None:
        """SYNC, dispatch-task ordering. Default: correlation only.

        Drivers override and decide state-update vs ``pending.feed`` ordering.
        """
        self.pending.feed(frame)

    def on_disconnect(self, exc: Exception | None) -> None:
        """Optional; runs before reconnect (``exc``) and on stop (``None``)."""

    def copy_state(self, state: S) -> S:
        """Snapshot for subscribers; default deepcopy, drivers override."""
        return copy.deepcopy(state)

    # ---- kit-provided facilities ----

    def subscribe(
        self, cb: Callable[[S | None], None]
    ) -> Callable[[], None]:
        """Register ``cb`` for state snapshots; returns an unsubscribe fn."""
        self._subscribers.append(cb)

        def unsubscribe() -> None:
            try:
                self._subscribers.remove(cb)
            except ValueError:
                pass

        return unsubscribe

    def notify(self) -> None:
        """Request a coalesced ``copy_state`` snapshot -> subscribers.

        Any number of ``notify()`` calls within one dispatch turn (or one
        :meth:`batch` block) deliver at most ONE notification, flushed via
        ``call_soon`` after the turn's synchronous work completes. Legal from
        the dispatch task (``on_frame``) and from caller tasks (command
        methods).
        """
        self._notify_dirty = True
        if not self._notify_scheduled:
            self._notify_scheduled = True
            asyncio.get_running_loop().call_soon(self._flush_notify)

    @contextmanager
    def batch(self) -> Iterator[None]:
        """Suppress notification flushes for the block, then flush once.

        Sugar for a burst of awaited requests (denon-style ``query_state`` of
        ~16 commands) that would otherwise deliver one notification per
        response. ``notify()`` calls inside the block set the dirty flag; the
        single coalesced snapshot is delivered when the last active batch
        exits. Ref-counted, so nested or concurrent batches (e.g. a manual
        poll racing a reconnect handshake) don't flush each other's window
        early — the flush waits until every batch has closed.
        """
        self._batch_depth += 1
        try:
            yield
        finally:
            self._batch_depth -= 1
            if self._batch_depth == 0:
                self._flush_notify()

    async def send(self, frame: bytes, *, pace: float | None = None) -> None:
        """Paced write (awaits the paced write itself)."""
        if not self.connected:
            raise ConnectionLostError("not connected")
        session = self._session_id
        async with self._pacing.send_slot(frame, pace=pace):
            # The connection may have dropped (and possibly reconnected) while
            # we were queued behind pacing; never write onto a different
            # session's writer.
            if not self.connected or self._session_id != session:
                raise ConnectionLostError("connection changed before write")
            self._writer.write(frame)
            await self._drain()

    async def request(
        self,
        frame: bytes,
        matcher: Matcher,
        *,
        timeout: float | None = None,
        pace: float | None = None,
    ) -> bytes:
        """Slot-gated, paced request: returns the correlated response frame.

        Pipeline order (normative): ``await pending.add()`` (slot + timeout
        clock start) -> paced write WITH done-check abandon -> ``await future``.
        """
        if not self.connected:
            raise ConnectionLostError("not connected")
        session = self._session_id
        future = await self.pending.add(
            matcher,
            timeout=self.request_timeout if timeout is None else timeout,
        )
        try:
            async with self._pacing.send_slot(frame, pace=pace):
                # The timeout timer started in add(); if it fired (or the
                # connection died) while we were queued behind pacing, the
                # write MUST be abandoned — emitting it would put an untracked
                # command on the wire (sony desync). Likewise if a reconnect
                # happened underneath us: this future belongs to the old
                # session's tracker, so never write it onto the new writer.
                if future.done():
                    pass
                elif not self.connected or self._session_id != session:
                    future.set_exception(
                        ConnectionLostError("connection changed before write")
                    )
                else:
                    self._writer.write(frame)
                    await self._drain()
        except asyncio.CancelledError:
            future.cancel()
            raise
        except Exception as exc:
            if not future.done():
                future.set_exception(
                    ConnectionLostError(f"write failed: {exc!r}")
                )
        return await future

    async def start(self) -> None:
        """connect -> make_state -> dispatch task -> on_connect -> notify.

        A failure connecting or in the initial ``on_connect()`` propagates out
        of ``start()`` (after symmetric teardown); the reconnect loop only
        supervises sessions that started successfully.
        """
        if self._monitor_task is not None:
            raise RuntimeError("already started")
        self._stopped = False
        await self._open_session()
        try:
            await self.on_connect()
        except BaseException:
            await self._teardown_session(None)
            raise
        self.notify()
        self._monitor_task = asyncio.create_task(self._monitor())

    async def stop(self) -> None:
        """Symmetric teardown: no reconnect, no un-awaited futures.

        ``fail_all(ConnectionLostError)`` -> ``on_disconnect(None)`` ->
        ``notify(None)``.
        """
        if self._stopped:
            return
        self._stopped = True
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except (asyncio.CancelledError, Exception):
                pass
            self._monitor_task = None
        if self._dispatch_task is not None or self.connected:
            await self._teardown_session(None)

    # ---- internals ----

    async def _drain(self) -> None:
        """Await the writer's flow control if it exposes ``drain``.

        A duck-typed ``StreamWriter.drain()`` applies backpressure so a burst
        of writes can't outrun the OS/proxy buffer; on transports without one
        (test doubles) this is a no-op.
        """
        writer = self._writer
        drain = getattr(writer, "drain", None)
        if drain is not None:
            await drain()

    def _new_framer(self) -> Framer:
        # Read off the class, not the instance, so a plain-function factory is
        # never bound to self (that is the staticmethod wart).
        spec = type(self).framer_factory
        if callable(spec):
            return spec()
        # A Framer instance used as a prototype: a fresh, empty copy each time.
        fresh = copy.deepcopy(spec)
        fresh.reset()
        return fresh

    async def _open_session(self) -> None:
        reader, writer = await self._connect()
        loop = asyncio.get_running_loop()
        self._session_id += 1
        # Rebuild pending per connection so a caller still queued on the old
        # session's slot gate can never have its future resolved by the new
        # session's frames (state is likewise rebuilt below).
        self.pending = PendingTracker(max_in_flight=self.max_in_flight)
        self._reader = reader
        self._writer = writer
        self._framer = self._new_framer()
        self.state = self.make_state()
        self._session_lost = loop.create_future()
        self._last_rx = loop.time()
        self.connected = True
        self._dispatch_task = asyncio.create_task(self._dispatch())
        if self.probe is not None:
            self._probe_task = asyncio.create_task(self._probe_loop())

    async def _teardown_session(self, exc: Exception | None) -> None:
        """``fail_all`` -> ``on_disconnect(exc)`` -> ``notify(None)``."""
        self.connected = False
        self._notify_dirty = False  # a pending snapshot must not outrun None
        for attr in ("_probe_task", "_dispatch_task"):
            task: asyncio.Task[None] | None = getattr(self, attr)
            setattr(self, attr, None)
            if task is not None and task is not asyncio.current_task():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:
                pass
            self._writer = None
        self._reader = None
        if exc is None:
            fail: Exception = ConnectionLostError("device stopped")
        elif isinstance(exc, ConnectionLostError):
            fail = exc
        else:
            fail = ConnectionLostError(f"connection lost: {exc!r}")
        self.pending.fail_all(fail)
        try:
            self.on_disconnect(exc)
        except Exception:
            pass  # driver bug in on_disconnect must not break teardown
        self._deliver(None)

    async def _monitor(self) -> None:
        """Kit-owned reconnect loop; cancelled by ``stop()``."""
        while True:
            assert self._session_lost is not None
            exc: Exception = await self._session_lost
            await self._teardown_session(exc)
            tries = 0
            while True:
                await asyncio.sleep(self.backoff.delay(tries))
                tries += 1
                try:
                    await self._open_session()
                except Exception:
                    continue  # connect factory failed; keep backing off
                try:
                    await self.on_connect()
                except Exception as handshake_exc:
                    # Handshake failed on the fresh connection: tear it down
                    # (delivers another notify(None)) and retry with backoff.
                    await self._teardown_session(handshake_exc)
                    continue
                self.notify()
                break

    async def _dispatch(self) -> None:
        """The single dispatch task: read -> framer -> on_frame per frame."""
        loop = asyncio.get_running_loop()
        try:
            while True:
                data = await self._reader.read(_READ_CHUNK)
                if not data:
                    raise ConnectionLostError("EOF from device")
                self._last_rx = loop.time()  # any RX counts as liveness
                self._process(data)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._report_session_loss(exc)

    def _process(self, data: bytes) -> None:
        """One dispatch turn: frame the chunk, route each frame, hardened."""
        assert self._framer is not None
        try:
            frames = list(self._framer.feed(data))
        except ResyncError as exc:
            # The framer never self-resets; the dispatch loop owns reset() and
            # still routes the frames completed before the desync.
            self._framer.reset()
            frames = list(exc.frames)
            self.frame_errors.append((b"", exc))
        for frame in frames:
            try:
                self.on_frame(frame)
            except Exception as exc:  # noqa: BLE001 - per-frame hardening
                self.frame_errors.append((frame, exc))

    async def _probe_loop(self) -> None:
        """Any-RX-alive watchdog in idle-window checkpoints.

        Each ``spec.idle`` window is judged by whether ANY RX arrived during
        it (checkpoint compare on ``_last_rx``, immune to clock jitter): RX ->
        misses reset; a fully silent window -> send a probe; after ``attempts``
        unanswered probes the next silent window declares the connection dead.
        """
        spec = self.probe
        assert spec is not None
        misses = 0
        while True:
            checkpoint = self._last_rx
            await asyncio.sleep(spec.idle)
            if self._last_rx > checkpoint:
                misses = 0  # link is alive; nothing owed
                continue
            if misses >= spec.attempts:
                self._report_session_loss(
                    ConnectionLostError(
                        f"probe unanswered after {misses} attempts"
                    )
                )
                return
            misses += 1
            try:
                await self.send(spec.frame)
            except Exception as exc:
                self._report_session_loss(
                    ConnectionLostError(f"probe write failed: {exc!r}")
                )
                return

    def _report_session_loss(self, exc: Exception) -> None:
        # set_result (not set_exception) so an unretrieved future can never
        # log "exception was never retrieved".
        if self._session_lost is not None and not self._session_lost.done():
            self._session_lost.set_result(exc)

    def _flush_notify(self) -> None:
        self._notify_scheduled = False
        if self._batch_depth:
            return  # keep the dirty flag; flushed when the last batch exits
        if not self._notify_dirty:
            return
        self._notify_dirty = False
        if not self.connected or self.state is None:
            return  # disconnected since notify(); None already delivered
        self._deliver(self.copy_state(self.state))

    def _deliver(self, snapshot: S | None) -> None:
        for cb in list(self._subscribers):
            try:
                cb(snapshot)
            except Exception:
                pass  # subscriber bugs must not break dispatch/teardown
