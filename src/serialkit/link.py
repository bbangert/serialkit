"""serialkit.link: the ``SerialLink`` runtime.

One dispatch task per connection reads the transport, frames each chunk, and
calls the driver's sync ``on_frame`` once per frame in order (exception
hardened), then ``on_turn`` once for the chunk. A kit-owned supervisor
reconnects with backoff and re-runs ``on_connect`` on every connection, which
is where a driver re-queries the device — so stale data is a protocol problem
solved by asking again, not a state-lifecycle problem.

The link is not generic and holds no device data. It never sees a command, a
response, or a state object; the only thing it knows that a driver cannot
compute for itself is where a dispatch turn ends, and that is ``on_turn``.

Pinned semantics:

- ``send`` / ``sweep`` / ``confirm`` / ``exchange`` raise
  :class:`~serialkit.ConnectionLostError` immediately when not connected —
  before ``start()``, during backoff, after ``stop()``. Nothing queues across a
  reconnect: a volume command delivered 60 seconds late is wrong for RS232.
- A write is abandoned if the session changed while it was queued behind
  pacing, so a stale frame never lands on a new session's writer.
- Reconnect fails all in-flight waits with ``ConnectionLostError`` *before*
  ``on_disconnect``.
- A framer ``ResyncError`` resets the framer from the **runtime** — never the
  framer itself — and the frames completed before the desync are still routed.
- ``on_frame`` exceptions are recorded in the bounded ``frame_errors`` and
  swallowed; they never kill the loop or the next frame.
- ``on_turn`` fires once per chunk that produced at least one frame, and not at
  all for a chunk that produced none.

``sweep`` / ``confirm`` / ``Exchange.next`` take an explicit ``timeout`` rather
than leaving it to ``asyncio.timeout`` at the call site (the ``ASYNC109``
suppressions below). How long a given device takes to answer a given command is
device knowledge the driver already holds; pushing the deadline out to every
call site would make each caller re-derive it, and a missed one would hang the
wire lock rather than fail.
"""

from __future__ import annotations

import asyncio
import copy
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Protocol

from .errors import CommandTimeoutError, ConnectionLostError, ResyncError
from .framing import Framer
from .pacing import Pacing
from .waiting import ExchangeContext, Match, WaitRegistry

_LOGGER = logging.getLogger(__name__)

_READ_CHUNK = 4096
_MAX_FRAME_ERRORS = 64

Connect = Callable[[], Awaitable[tuple[Any, Any]]]


@dataclass(frozen=True)
class Backoff:
    """Reconnect backoff policy: ``initial * factor**tries``, capped."""

    initial: float = 0.5
    factor: float = 1.8
    max_delay: float = 60.0

    def delay(self, tries: int) -> float:
        return min(self.initial * self.factor**tries, self.max_delay)


@dataclass(frozen=True)
class IdleProbe:
    """Liveness for a device whose silence is meaningful, or that can be poked.

    Judged by idle-window checkpoints — "was there any RX during this window?"
    — never by a ``now - last_rx`` clock delta, which is flaky under scheduling
    jitter and reconnects healthy devices. Any received data counts as alive,
    not just a frame that answers the probe.

    ``probe=None`` is passive: a device that reports continuously needs no
    poke, and ``attempts`` silent windows alone declare it dead. ``attempts``
    covers a standby MCU that consumes the first frame waking up.
    """

    idle: float
    probe: bytes | None = None
    attempts: int = 3


@dataclass(frozen=True)
class FailureCount:
    """Liveness for a device that emits nothing unsolicited.

    Silence is such a device's resting state, so passive detection is
    meaningless; the only evidence of a dead link is that commands stop being
    answered. ``consecutive`` timeouts from ``confirm`` or ``Exchange.next``
    trip a reconnect. Any answered command resets the count, including one for
    a different command.
    """

    consecutive: int = 3


Liveness = IdleProbe | FailureCount

#: The default reconnect policy. Frozen, so one shared instance is safe.
_DEFAULT_BACKOFF = Backoff()


class DeviceHandler(Protocol):
    """What a driver implements. Four plain callbacks, no types threaded
    through, mirroring ``asyncio.Protocol``'s connection_made / data_received /
    connection_lost.

    ``on_frame`` and ``on_connect`` are required. ``on_turn`` and
    ``on_disconnect`` are optional — the link probes for them and skips what a
    handler does not define.
    """

    def on_frame(self, frame: bytes) -> None:
        """Sync, on the dispatch task, once per frame in order.

        Decode the frame, apply it to the driver's model, queue an event. An
        exception here is recorded in ``frame_errors`` and swallowed.
        """
        ...

    async def on_connect(self) -> None:
        """Runs on every connection, with frames already flowing — so
        ``send`` / ``sweep`` / ``expect`` / ``exchange`` all work inside it.
        This is where a driver runs its full re-query.

        Raising fails the connection: it propagates out of ``start()`` on the
        first attempt, and triggers backoff-and-retry on a reconnect.
        """
        ...


class SerialLink:
    """The runtime: connect, dispatch, reconnect, pace, wait.

    The transport is injected as an async ``connect`` factory returning a
    duck-typed ``(reader, writer)`` pair — ``reader.read(n) -> bytes`` (empty
    means EOF), ``writer.write(bytes)``, ``writer.close()``, and optionally
    ``await writer.drain()`` — so the runtime is testable without hardware and
    works with any ``serialx`` URL (local, ``socket://``, ``esphome://``).

    A driver **owns** a link and implements :class:`DeviceHandler`. That keeps
    the kit out of the driver's namespace, makes per-instance config (a
    model-dependent baud rate) ordinary, and lets each library name its own
    public API.
    """

    def __init__(
        self,
        *,
        connect: Connect,
        framer: Framer,
        handler: DeviceHandler,
        pacing: Pacing | None = None,
        liveness: Liveness | None = None,
        backoff: Backoff = _DEFAULT_BACKOFF,
        connect_timeout: float = 10.0,
        time_func: Callable[[], float] | None = None,
        sleep_func: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._connect = connect
        # A prototype, deep-copied and reset per connection, so no
        # connection inherits another's residual buffer.
        self._framer_prototype = framer
        self._handler = handler
        self._pacing = pacing or Pacing()
        self._liveness = liveness
        self._backoff = backoff
        self._connect_timeout = connect_timeout
        self._time_func = time_func
        self._sleep_func = sleep_func

        # Optional callbacks: probed once, so a handler may omit them.
        self._on_turn: Callable[[], None] | None = getattr(handler, "on_turn", None)
        self._on_disconnect: Callable[[Exception | None], None] | None = getattr(
            handler, "on_disconnect", None
        )

        self.connected = False
        #: Increments per successful connection. Diagnostic only — consumers do
        #: not need it, because ``on_connect`` always re-queries.
        self.session = 0
        #: ``(frame, exc)`` per ``on_frame`` crash, and ``(b"", ResyncError)``
        #: per framer desync. Bounded: an unbounded list grows forever under
        #: Home Assistant's multi-month uptimes.
        self.frame_errors: deque[tuple[bytes, Exception]] = deque(
            maxlen=_MAX_FRAME_ERRORS
        )

        self._registry = WaitRegistry()
        self._send_lock = asyncio.Lock()
        self._wire_lock = asyncio.Lock()
        self._next_allowed = float("-inf")
        self._consecutive_failures = 0
        # Counts read chunks, not frames: ANY received data proves the link is
        # alive. A counter rather than a timestamp keeps the idle-window
        # comparison independent of which clock is driving the sleep.
        self._rx_chunks = 0

        self._reader: Any = None
        self._writer: Any = None
        self._framer: Framer | None = None
        self._dispatch_task: asyncio.Task[None] | None = None
        self._liveness_task: asyncio.Task[None] | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        self._session_lost: asyncio.Future[Exception] | None = None
        self._stopped = False

    # ---- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        """Connect, start dispatch, run ``on_connect``, then supervise.

        A failure connecting or in the first ``on_connect()`` propagates out of
        ``start()`` after symmetric teardown; the reconnect loop only
        supervises a session that started successfully.
        """
        if self._monitor_task is not None:
            raise RuntimeError("already started")
        self._stopped = False
        await self._open_session()
        try:
            await self._handler.on_connect()
        except BaseException:
            await self._teardown_session(None)
            raise
        self._monitor_task = asyncio.create_task(self._monitor())

    async def stop(self) -> None:
        """Symmetric teardown: no reconnect, no un-awaited futures."""
        if self._stopped:
            return
        self._stopped = True
        if self._monitor_task is not None:
            task, self._monitor_task = self._monitor_task, None
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        if self._dispatch_task is not None or self.connected:
            await self._teardown_session(None)

    # ---- sending --------------------------------------------------------

    async def send(self, frame: bytes, *, pace: float | None = None) -> None:
        """Write ``frame``, holding the pacing interval it selects."""
        await self._write(frame, pace=pace)

    async def sweep(
        self,
        frames: Sequence[bytes],
        *,
        quiet: float = 0.3,
        timeout: float = 5.0,  # noqa: ASYNC109
        pace: float | None = None,
    ) -> None:
        """Send every frame under pacing, then wait for replies to stop.

        The bulk refresh for a publisher: nudge the device for everything, then
        wait until ``quiet`` seconds pass with no RX (or ``timeout`` total
        elapses). Waiting for silence rather than sleeping a fixed amount
        adapts to a slow device.

        There is **no per-frame success or failure**. Frames arriving during
        the sweep go through ``on_frame`` normally and ``on_turn`` fires per
        chunk, so a driver's own event batching already collapses a sweep into
        few notifications. An unanswered nudge is simply a field that never
        gets an event.
        """
        for frame in frames:
            await self._write(frame, pace=pace)
        deadline = self._now() + timeout
        while await self._rx_during(quiet):
            if self._now() >= deadline:
                _LOGGER.debug(
                    "sweep hit its %ss budget with replies still arriving",
                    timeout,
                )
                return

    # ---- waiting --------------------------------------------------------

    def expect(self, match: Match, *, timeout: float) -> Awaitable[bytes]:
        """Arm a waiter NOW for the next frame satisfying ``match``.

        Arming is synchronous and happens at call time, so the caller can arm
        *before* the send and a reply in the same read chunk as the write is
        never lost::

            waiter = link.expect(is_power_report, timeout=1.0)
            await link.send(POWER_ON)
            frame = await waiter

        This is observation, not correlation: the frame is often an unsolicited
        auto-report that answers nothing in particular. It **observes without
        consuming** — a frame that satisfies the predicate is normally also a
        state event the driver must apply, so it still reaches ``on_frame``.
        Only :meth:`exchange` claims a frame.
        """
        return self._registry.arm(match, timeout=timeout)

    async def confirm(
        self,
        *,
        nudge: bytes,
        match: Match,
        timeout: float,  # noqa: ASYNC109
        retries: int = 0,
        pace: float | None = None,
    ) -> bytes:
        """Arm, send, wait, and retry — delivery confirmed by the device.

        A frame that has not arrived yet cannot already be true, so this is an
        honest confirmation rather than a check that can pass vacuously against
        a state that was already correct.

        ``retries`` is device knowledge the driver supplies: a standby MCU that
        consumes the first frame waking up needs its command sent twice, and
        ``retries=1`` is exactly that.
        """
        last: Exception | None = None
        for _ in range(retries + 1):
            waiter = self.expect(match, timeout=timeout)
            try:
                await self._write(nudge, pace=pace)
            except BaseException:
                self._registry.cancel(waiter)
                raise
            try:
                frame = await waiter
            except CommandTimeoutError as exc:
                last = exc
                continue
            self._note_command_result(timed_out=False)
            return frame
        self._note_command_result(timed_out=True)
        assert last is not None
        raise last

    def exchange(self) -> ExchangeContext:
        """Hold the wire exclusively for one send-and-read round.

        For a transactional device whose replies carry no function identifier
        and are only decodable against the outstanding command. Unlike
        :meth:`expect`, an exchange **claims** the frames it takes: they do not
        reach ``on_frame``. ::

            async with link.exchange() as ex:
                await ex.send(encode_query(fn))
                frame = await ex.next(timeout=2.0)
        """
        if not self.connected:
            raise ConnectionLostError("not connected")
        return ExchangeContext(
            lock=self._wire_lock,
            registry=self._registry,
            write=self._write,
            on_result=self._note_command_result,
            release=self._deliver_frames,
        )

    def report_error(self, exc: Exception) -> None:
        """Fail every in-flight wait now, rather than waiting out its timeout.

        A driver calls this from ``on_frame`` when the device reports an error
        that makes the outstanding wait pointless.
        """
        self._registry.fail_all(exc)

    # ---- internals: writing ---------------------------------------------

    async def _write(
        self,
        frame: bytes,
        *,
        pace: float | None = None,
        before_write: Callable[[], None] | None = None,
    ) -> None:
        if not self.connected:
            raise ConnectionLostError("not connected")
        session = self.session
        async with self._send_lock:
            now = self._now()
            if self._next_allowed > now:
                await self._sleep(self._next_allowed - now)
            try:
                # The connection may have dropped — and possibly reconnected —
                # while this frame waited its pacing turn. Never write onto a
                # different session's writer.
                if not self.connected or self.session != session:
                    raise ConnectionLostError("connection changed before write")
                if before_write is not None:
                    before_write()
                self._writer.write(frame)
                await self._drain()
            finally:
                # An abandoned write still reserves the slot: conservative and
                # harmless, and it keeps the timestamp update on one path.
                self._next_allowed = self._now() + self._pacing.interval_for(
                    frame, pace=pace
                )

    async def _drain(self) -> None:
        """Await the writer's flow control if it exposes ``drain``.

        Backpressure so a burst of writes cannot outrun the OS or proxy buffer;
        a no-op on transports without one.
        """
        drain = getattr(self._writer, "drain", None)
        if drain is not None:
            await drain()

    # ---- internals: session ---------------------------------------------

    def _new_framer(self) -> Framer:
        fresh = copy.deepcopy(self._framer_prototype)
        fresh.reset()
        return fresh

    async def _open_session(self) -> None:
        # A device path that blocks on open (flow control, DCD) would otherwise
        # wedge the reconnect loop forever: serialx has no connect timeout on
        # fd backends.
        async with asyncio.timeout(self._connect_timeout):
            reader, writer = await self._connect()
        self.session += 1
        self._reader = reader
        self._writer = writer
        self._framer = self._new_framer()
        self._registry = WaitRegistry()
        self._next_allowed = float("-inf")
        self._consecutive_failures = 0
        self._session_lost = asyncio.get_running_loop().create_future()
        self.connected = True
        self._dispatch_task = asyncio.create_task(self._dispatch())
        if self._liveness is not None:
            self._liveness_task = asyncio.create_task(self._liveness_loop())

    async def _teardown_session(self, exc: Exception | None) -> None:
        self.connected = False
        for attr in ("_liveness_task", "_dispatch_task"):
            task: asyncio.Task[None] | None = getattr(self, attr)
            setattr(self, attr, None)
            if task is not None and task is not asyncio.current_task():
                task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await task
        if self._writer is not None:
            with suppress(Exception):
                self._writer.close()
            self._writer = None
        self._reader = None
        # Whatever the transport raised — a bare OSError from an unplugged
        # serial device, a TimeoutError — becomes a ConnectionLostError with
        # the original chained, so a driver and its consumer only ever handle
        # kit-level types. on_disconnect(None) means stop(), not a drop.
        if exc is None:
            fail: Exception = ConnectionLostError("link stopped")
            reason: Exception | None = None
        elif isinstance(exc, ConnectionLostError):
            fail = reason = exc
        else:
            fail = ConnectionLostError(f"connection lost: {exc!r}")
            fail.__cause__ = exc
            reason = fail
        self._registry.fail_all(fail)
        if self._on_disconnect is not None:
            try:
                self._on_disconnect(reason)
            except Exception:
                _LOGGER.exception("on_disconnect raised; continuing teardown")

    async def _monitor(self) -> None:
        """Kit-owned reconnect supervisor; cancelled by ``stop()``."""
        while True:
            assert self._session_lost is not None
            exc: Exception = await self._session_lost
            await self._teardown_session(exc)
            tries = 0
            while True:
                await self._sleep(self._backoff.delay(tries))
                tries += 1
                try:
                    await self._open_session()
                except Exception as connect_exc:  # noqa: BLE001
                    # Any failure to open — a missing port, a refused socket, a
                    # connect that timed out — is the same answer here: not yet.
                    # Logged rather than swallowed, because a link that never
                    # comes back is otherwise silent about why.
                    _LOGGER.debug("reconnect attempt %d failed: %r", tries, connect_exc)
                    continue
                try:
                    await self._handler.on_connect()
                except Exception as handshake_exc:  # noqa: BLE001
                    # A driver's handshake may raise anything at all; it means
                    # this connection is unusable, not that the loop is over.
                    _LOGGER.debug(
                        "handshake failed on attempt %d: %r", tries, handshake_exc
                    )
                    await self._teardown_session(handshake_exc)
                    continue
                break

    # ---- internals: dispatch --------------------------------------------

    async def _dispatch(self) -> None:
        """The single dispatch task: read -> frame -> route -> on_turn."""
        try:
            while True:
                # EOF is b"" on socket-family transports but a raised OSError
                # on a real serial device being unplugged; both mean the same
                # thing here.
                data = await self._reader.read(_READ_CHUNK)
                if not data:
                    raise ConnectionLostError("EOF from device")
                self._rx_chunks += 1
                self._process(data)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            # The read loop must convert ANY transport error into a session
            # loss — the whole point is that the supervisor, not the caller,
            # decides what a broken link means.
            self._report_session_loss(exc)

    def _process(self, data: bytes) -> None:
        """One dispatch turn: frame the chunk, route each frame, hardened."""
        assert self._framer is not None
        try:
            frames = list(self._framer.feed(data))
        except ResyncError as exc:
            # The framer never resets itself; the runtime owns reset() and
            # still routes whatever completed before the desync.
            self._framer.reset()
            frames = list(exc.frames)
            self.frame_errors.append((b"", exc))
        self._deliver_frames(frames)

    def _deliver_frames(self, frames: Sequence[bytes]) -> None:
        dispatched = 0
        for frame in frames:
            if self._registry.route(frame):
                continue  # claimed by an open exchange
            try:
                self._handler.on_frame(frame)
            except Exception as exc:  # noqa: BLE001
                # Per-frame hardening: a driver crash decoding one frame must
                # never kill the loop or the next frame. Recorded, not raised.
                self.frame_errors.append((frame, exc))
            dispatched += 1
        # Only a chunk that actually reached the driver is a turn worth
        # flushing; a zero-frame chunk (or one wholly claimed by an exchange)
        # must not fire it.
        if dispatched and self._on_turn is not None:
            try:
                self._on_turn()
            except Exception:
                _LOGGER.exception("on_turn raised; dispatch continues")

    def _report_session_loss(self, exc: Exception) -> None:
        # set_result, not set_exception, so an unretrieved future can never log
        # "exception was never retrieved".
        if self._session_lost is not None and not self._session_lost.done():
            self._session_lost.set_result(exc)

    # ---- internals: liveness --------------------------------------------

    async def _rx_during(self, window: float) -> bool:
        """Sleep ``window``; True if any RX arrived during it.

        The one idle-window checkpoint, shared by :meth:`sweep`'s quiet window
        and the :class:`IdleProbe` watchdog — both are asking the same
        question.
        """
        checkpoint = self._rx_chunks
        await self._sleep(window)
        return self._rx_chunks > checkpoint

    async def _liveness_loop(self) -> None:
        if isinstance(self._liveness, IdleProbe):
            await self._idle_probe_loop(self._liveness)
        # FailureCount needs no loop: it trips from command results.

    async def _idle_probe_loop(self, spec: IdleProbe) -> None:
        misses = 0
        while True:
            if await self._rx_during(spec.idle):
                misses = 0  # the link is alive; nothing owed
                continue
            if misses >= spec.attempts:
                self._report_session_loss(
                    ConnectionLostError(f"no RX after {misses} probe attempts")
                )
                return
            misses += 1
            if spec.probe is None:
                continue  # passive: silence alone counts down
            try:
                await self.send(spec.probe)
            except Exception as exc:  # noqa: BLE001
                # If we cannot even poke the device, the link is already gone;
                # any error here means the same thing.
                self._report_session_loss(
                    ConnectionLostError(f"probe write failed: {exc!r}")
                )
                return

    def _note_command_result(self, *, timed_out: bool) -> None:
        if not isinstance(self._liveness, FailureCount):
            return
        if not timed_out:
            self._consecutive_failures = 0
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._liveness.consecutive:
            self._report_session_loss(
                ConnectionLostError(
                    f"{self._consecutive_failures} consecutive command timeouts"
                )
            )

    # ---- internals: the clock seam --------------------------------------

    def _now(self) -> float:
        if self._time_func is not None:
            return self._time_func()
        return asyncio.get_running_loop().time()

    async def _sleep(self, delay: float) -> None:
        if self._sleep_func is not None:
            await self._sleep_func(delay)
        else:
            await asyncio.sleep(delay)


__all__ = [
    "Backoff",
    "DeviceHandler",
    "FailureCount",
    "IdleProbe",
    "SerialLink",
]
