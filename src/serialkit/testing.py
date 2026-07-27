"""serialkit.testing: in-memory transport doubles for driver tests.

In-memory doubles for driver tests. Nothing here touches real hardware:

- :class:`FakeLink` is an injected ``connect`` factory (pass ``link.connect``
  to a :class:`~serialkit.SerialLink`). It hands out a fresh reader/writer pair
  per connection, records every written frame, scripts device responses, and
  injects the transport fault shapes below.
- :class:`FakeClock` drives the link's ``time_func``/``sleep_func`` seam, so
  pacing, backoff, and liveness windows resolve instantly instead of waiting.

The fault shapes mirror real ``serialx`` behaviour, so a recovery path is
exercised in the form it actually takes on hardware:

============================== ==========================================
:meth:`FakeLink.drop`          EOF (``b""``) — the socket-family graceful
                               FIN shape.
:meth:`FakeLink.drop`
``(abrupt=True)``              ``read()`` raises ``OSError(EIO)`` — the real
                               serial-unplug shape.
:meth:`FakeLink.fail_writes`
``(silent=True)``              writes accepted and discarded, reproducing the
                               fd transport where a failed ``os.write``
                               returns normally and the error surfaces one
                               call later.
:meth:`FakeLink.fail_writes`   ``write()`` raises immediately.
:meth:`FakeLink.hang_connect`  a ``connect`` factory that never resolves.
============================== ==========================================

Typical driver test::

    link = FakeLink()
    link.respond({b"POW?\\r": b"POW1\\r"})   # script an answer
    dev = MyDevice(link.connect)
    await dev.start()
    await dev.query_power()
    assert link.sent == [b"POW?\\r"]
"""

from __future__ import annotations

import asyncio
import errno
from collections.abc import Awaitable, Callable, Mapping


class FakeClock:
    """Deterministic clock: ``sleep`` advances virtual time instead of waiting.

    Wire it into a link with
    ``SerialLink(..., time_func=clock.time, sleep_func=clock.sleep)``. It
    covers the sleeps the kit *initiates* — pacing, backoff, the liveness idle
    window, the sweep quiet window. Deadlines on external events (``expect`` /
    ``confirm`` / ``Exchange.next`` timeouts) deliberately stay on loop time:
    a clock whose ``sleep`` returns immediately would win every race against a
    real future and fire every timeout instantly.
    """

    #: Loop turns yielded per virtual sleep. Advancing the clock has to let
    #: everything that is runnable at the new time actually run — a dispatch
    #: task with bytes waiting, a script feeding the next reply — or a window
    #: that virtually lasted a minute would be judged before any of it moved.
    turns_per_sleep = 10

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay
        for _ in range(self.turns_per_sleep):
            await asyncio.sleep(0)


class FakeReader:
    """Duck-typed ``StreamReader``: ``read()`` pops injected chunks.

    A queued ``b""`` signals EOF; a queued exception is raised from ``read()``
    (the abrupt-unplug shape).
    """

    def __init__(self) -> None:
        self._chunks: asyncio.Queue[bytes | Exception] = asyncio.Queue()

    def feed(self, data: bytes) -> None:
        self._chunks.put_nowait(bytes(data))

    def feed_eof(self) -> None:
        self._chunks.put_nowait(b"")

    def feed_error(self, exc: Exception) -> None:
        self._chunks.put_nowait(exc)

    async def read(self, _n: int = -1) -> bytes:
        item = await self._chunks.get()
        if isinstance(item, Exception):
            raise item
        return item


class FakeWriter:
    """Duck-typed ``StreamWriter``: records writes, with an optional
    ``on_write`` hook used to script synchronous device responses."""

    def __init__(self, on_write: Callable[[bytes], None] | None = None) -> None:
        self.written: list[bytes] = []
        self.drains = 0
        self.closed = False
        self.discarded: list[bytes] = []
        self._on_write = on_write
        # Set by FakeLink.fail_writes().
        self.write_error: Exception | None = None
        self.silent_write_failure = False

    def write(self, data: bytes) -> None:
        if self.closed:
            raise RuntimeError("write to closed writer")
        if self.silent_write_failure:
            # The fd-transport shape: os.write failed, the exception was
            # swallowed, and write()/drain() both return normally. The frame
            # never reaches the device, and nothing here reports it.
            self.discarded.append(bytes(data))
            return
        if self.write_error is not None:
            raise self.write_error
        self.written.append(bytes(data))
        if self._on_write is not None:
            self._on_write(bytes(data))

    async def drain(self) -> None:
        self.drains += 1

    def close(self) -> None:
        self.closed = True


class FakeLink:
    """Injected async connect factory: a fresh reader/writer per call.

    - ``preload``: chunks pre-queued on every new reader (unsolicited data
      already on the wire when the connection opens).
    - ``on_write`` / :meth:`respond`: script device responses to written
      frames (echo transports).
    - :meth:`rx` / :meth:`garble`: inject received bytes or a corrupt burst.
    - :meth:`drop` / :meth:`fail_writes` / :meth:`hang_connect`: the transport
      fault shapes (see the module docstring).
    """

    def __init__(self, *, preload: list[bytes] | None = None) -> None:
        self.connects = 0
        self.readers: list[FakeReader] = []
        self.writers: list[FakeWriter] = []
        self.on_write: Callable[[bytes], None] | None = None
        self.connect_error: Exception | None = None
        self._preload = list(preload or [])
        self._hang = False

    async def connect(self) -> tuple[FakeReader, FakeWriter]:
        if self._hang:
            await asyncio.Event().wait()  # never resolves
        if self.connect_error is not None:
            raise self.connect_error
        self.connects += 1
        reader = FakeReader()
        for chunk in self._preload:
            reader.feed(chunk)
        writer = FakeWriter(self._dispatch_write)
        self.readers.append(reader)
        self.writers.append(writer)
        return reader, writer

    def respond(self, answers: Mapping[bytes, bytes]) -> None:
        """Script exact ``command -> response`` echoes.

        Installs an ``on_write`` hook that feeds ``answers[frame]`` back on the
        current reader whenever a written frame matches a key exactly.
        """
        table = dict(answers)

        def _echo(data: bytes) -> None:
            response = table.get(data)
            if response is not None:
                self.rx(response)

        self.on_write = _echo

    def _dispatch_write(self, data: bytes) -> None:
        if self.on_write is not None:
            self.on_write(data)

    @property
    def reader(self) -> FakeReader:
        return self.readers[-1]

    @property
    def writer(self) -> FakeWriter:
        return self.writers[-1]

    @property
    def sent(self) -> list[bytes]:
        """Frames written on the CURRENT connection."""
        return self.writers[-1].written

    def rx(self, data: bytes) -> None:
        """Inject received bytes on the current connection."""
        self.readers[-1].feed(data)

    def garble(self, data: bytes = b"\xff\xff\xff\xff") -> None:
        """Inject a corrupt byte burst (desync / resync tests)."""
        self.rx(data)

    def drop(self, *, abrupt: bool = False) -> None:
        """Simulate the device/link going away.

        ``abrupt=False`` (default) delivers EOF (``b""``), what socket-family
        transports do on a graceful FIN. ``abrupt=True`` raises
        ``OSError(EIO)`` out of ``read()``, what a real serial device does when
        it is unplugged — the shape the kit's recovery path must also handle.
        """
        if abrupt:
            self.readers[-1].feed_error(OSError(errno.EIO, "Input/output error"))
        else:
            self.readers[-1].feed_eof()

    def fail_writes(
        self, *, silent: bool = False, exc: Exception | None = None
    ) -> None:
        """Make writes on the current connection fail.

        ``silent=True`` reproduces the fd-transport behaviour where a failed
        ``os.write`` is swallowed: ``write()`` and ``drain()`` both return
        normally, the frame is discarded (recorded in ``writer.discarded``),
        and the device simply never sees it. ``silent=False`` raises ``exc``
        (default ``OSError(EIO)``) from ``write()``.
        """
        writer = self.writers[-1]
        if silent:
            writer.silent_write_failure = True
        else:
            writer.write_error = exc or OSError(errno.EIO, "Input/output error")

    def hang_connect(self) -> Callable[[], Awaitable[tuple[FakeReader, FakeWriter]]]:
        """Make :meth:`connect` never resolve, and return it.

        For exercising ``connect_timeout``: a device path that blocks on open
        (flow control / DCD) must not wedge the reconnect loop.
        """
        self._hang = True
        return self.connect

    def resume_connect(self) -> None:
        """Undo :meth:`hang_connect`, so the next attempt succeeds."""
        self._hang = False


__all__ = ["FakeClock", "FakeLink", "FakeReader", "FakeWriter"]
