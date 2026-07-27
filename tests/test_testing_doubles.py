"""The fault shapes ``serialkit.testing`` must be able to reproduce.

Every recovery path in the kit is only as trustworthy as the fault that
exercises it, and the shapes below are the ones real transports produce. These
tests pin the doubles themselves — that a drop is EOF *or* an ``OSError``, that
a silent write failure looks like success to the writer — so that a link test
asserting "the kit recovered" is asserting it against the real shape rather
than the convenient one.
"""

from __future__ import annotations

import asyncio
import errno

import pytest

from serialkit.testing import FakeClock, FakeLink


async def test_drop_delivers_eof(link: FakeLink) -> None:
    """The socket-family graceful-FIN shape: read() returns b""."""
    await link.connect()
    link.rx(b"DATA")
    link.drop()
    assert await link.reader.read() == b"DATA"
    assert await link.reader.read() == b""


async def test_abrupt_drop_raises_oserror(link: FakeLink) -> None:
    """The real serial-unplug shape: read() raises instead of returning b"".

    Reconnect is the kit's most important recovery path, so it has to be
    exercised in this form and not only the easier EOF one.
    """
    await link.connect()
    link.rx(b"DATA")
    link.drop(abrupt=True)
    assert await link.reader.read() == b"DATA"
    with pytest.raises(OSError) as excinfo:
        await link.reader.read()
    assert excinfo.value.errno == errno.EIO


async def test_silent_write_failure_looks_like_success(link: FakeLink) -> None:
    """The fd-transport shape: os.write failed, but write() and drain() both
    return normally, so the frame vanishes with no error anywhere."""
    await link.connect()
    seen: list[bytes] = []
    link.on_write = seen.append

    link.fail_writes(silent=True)
    link.writer.write(b"LOST")
    await link.writer.drain()  # no exception — that is the whole problem

    assert seen == []  # the device never saw it
    assert link.writer.written == []  # ...and it is not recorded as sent
    assert link.writer.discarded == [b"LOST"]


async def test_loud_write_failure_raises(link: FakeLink) -> None:
    await link.connect()
    link.fail_writes()
    with pytest.raises(OSError):
        link.writer.write(b"BOOM")


async def test_hang_connect_never_resolves(link: FakeLink) -> None:
    """A device path that blocks on open. Without connect_timeout this wedges
    the reconnect loop permanently."""
    connect = link.hang_connect()
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await connect()
    assert link.connects == 0

    link.resume_connect()
    await connect()
    assert link.connects == 1


async def test_fake_clock_advances_without_waiting() -> None:
    clock = FakeClock()
    started = asyncio.get_running_loop().time()
    await clock.sleep(60.0)
    await clock.sleep(30.0)
    assert clock.now == pytest.approx(90.0)
    assert clock.sleeps == [60.0, 30.0]
    # 90 virtual seconds cost no real ones.
    assert asyncio.get_running_loop().time() - started < 1.0
