"""serialkit.testing: in-memory transport doubles for driver tests.

Replaces the hand-rolled ``MockSerialConnection`` each device library ships.
Nothing here touches real hardware:

- :class:`FakeLink` is an injected ``connect`` factory (pass ``link.connect``
  to a :class:`~serialkit.SerialDevice`). It hands out a fresh reader/writer
  pair per connection, records every written frame, scripts device responses,
  and injects desync faults (dropped connection, garbled bytes).
- :class:`FakeClock` makes :class:`~serialkit.Pacing` deterministic by
  advancing a virtual clock on ``sleep`` instead of waiting.

Typical driver test::

    link = FakeLink()
    link.respond({b"POW?\\r": b"POW1\\r"})   # script an answer
    dev = MyDevice(link.connect)
    await dev.start()
    assert await dev.query_power() is PowerState.ON
    assert link.sent == [b"POW?\\r"]
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping


class FakeClock:
    """Deterministic clock: ``sleep`` advances virtual time instead of waiting.

    Wire it into :class:`~serialkit.Pacing` with
    ``Pacing(..., time_func=clock.time, sleep_func=clock.sleep)``.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay


class FakeReader:
    """Duck-typed ``StreamReader``: ``read()`` pops injected chunks; ``b""``
    signals EOF."""

    def __init__(self) -> None:
        self._chunks: asyncio.Queue[bytes] = asyncio.Queue()

    def feed(self, data: bytes) -> None:
        self._chunks.put_nowait(bytes(data))

    def feed_eof(self) -> None:
        self._chunks.put_nowait(b"")

    async def read(self, _n: int = -1) -> bytes:
        return await self._chunks.get()


class FakeWriter:
    """Duck-typed ``StreamWriter``: records writes, with an optional
    ``on_write`` hook used to script synchronous device responses."""

    def __init__(
        self, on_write: Callable[[bytes], None] | None = None
    ) -> None:
        self.written: list[bytes] = []
        self.drains = 0
        self.closed = False
        self._on_write = on_write

    def write(self, data: bytes) -> None:
        if self.closed:
            raise RuntimeError("write to closed writer")
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
    - :meth:`rx` / :meth:`garble` / :meth:`drop`: inject received bytes, a
      corrupt burst, or a connection loss for desync/reconnect tests.
    """

    def __init__(self, *, preload: list[bytes] | None = None) -> None:
        self.connects = 0
        self.readers: list[FakeReader] = []
        self.writers: list[FakeWriter] = []
        self.on_write: Callable[[bytes], None] | None = None
        self._preload = list(preload or [])

    async def connect(self) -> tuple[FakeReader, FakeWriter]:
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
        current reader whenever a written frame matches a key exactly. Replaces
        the per-test echo closures with a declarative table.
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

    def drop(self) -> None:
        """Simulate the device/link going away (EOF -> reconnect)."""
        self.readers[-1].feed_eof()


__all__ = ["FakeClock", "FakeLink", "FakeReader", "FakeWriter"]
