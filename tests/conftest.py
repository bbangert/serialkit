"""Test harness: fake clock for pacing, plus duck-typed reader/writer doubles
and a FakeLink connect factory for the SerialDevice runtime. No test requires
real hardware."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from serialkit import Backoff, DelimiterFramer, SerialDevice


class FakeClock:
    """Deterministic clock: sleep() advances time instead of waiting."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay


class FakeReader:
    """Duck-typed StreamReader: read() pops injected chunks; b"" = EOF."""

    def __init__(self) -> None:
        self._chunks: asyncio.Queue[bytes] = asyncio.Queue()

    def feed(self, data: bytes) -> None:
        self._chunks.put_nowait(bytes(data))

    def feed_eof(self) -> None:
        self._chunks.put_nowait(b"")

    async def read(self, _n: int = -1) -> bytes:
        return await self._chunks.get()


class FakeWriter:
    """Duck-typed StreamWriter: records writes, optional on_write hook."""

    def __init__(
        self, on_write: Callable[[bytes], None] | None = None
    ) -> None:
        self.written: list[bytes] = []
        self.closed = False
        self._on_write = on_write

    def write(self, data: bytes) -> None:
        if self.closed:
            raise RuntimeError("write to closed writer")
        self.written.append(bytes(data))
        if self._on_write is not None:
            self._on_write(bytes(data))

    def close(self) -> None:
        self.closed = True


class FakeLink:
    """Injected async connect factory: a fresh reader/writer per call.

    - ``preload``: chunks pre-queued on every new reader (unsolicited data
      already on the wire when the connection opens).
    - ``on_write``: called with each written frame; tests use it to script
      device responses (echo transports).
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

    def drop(self) -> None:
        """Simulate the device/link going away (EOF)."""
        self.readers[-1].feed_eof()


class DictDevice(SerialDevice[dict]):
    """Baseline test device: newline framing, dict state, tiny backoff."""

    framer_factory = staticmethod(lambda: DelimiterFramer(b"\n"))
    backoff = Backoff(initial=0.01, factor=1.0, max_delay=0.01)

    def make_state(self) -> dict:
        return {}

    def copy_state(self, state: dict) -> dict:
        return dict(state)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def link() -> FakeLink:
    return FakeLink()
