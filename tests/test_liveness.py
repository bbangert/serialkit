"""Liveness: the two shapes, and the idle-window checkpoint they share.

A publisher goes quiet when the link dies, so silence is evidence. A
transactional device is silent as its resting state, so silence proves
nothing and only unanswered commands do. Both run on the FakeClock seam, so a
60-second idle window costs no real time.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import Recorder

from serialkit import (
    Backoff,
    CommandTimeoutError,
    DelimiterFramer,
    FailureCount,
    IdleProbe,
    SerialLink,
)
from serialkit.testing import FakeClock, FakeLink

FAST = Backoff(initial=0.01, factor=1.0, max_delay=0.01)


def make_link(link: FakeLink, handler: object, **kwargs: object) -> SerialLink:
    kwargs.setdefault("backoff", FAST)
    return SerialLink(
        connect=link.connect,
        framer=DelimiterFramer(b"\n"),
        handler=handler,  # type: ignore[arg-type]
        **kwargs,  # type: ignore[arg-type]
    )


async def spin(check: object, tries: int = 400) -> None:
    """Let the loop run until ``check()`` holds (or we give up)."""
    for _ in range(tries):
        if check():  # type: ignore[operator]
            return
        await asyncio.sleep(0)


# ---- IdleProbe ----------------------------------------------------------


async def test_idle_probe_pokes_a_quiet_link_and_an_answer_keeps_it_alive(
    link: FakeLink, handler: Recorder, clock: FakeClock
) -> None:
    dev = make_link(
        link,
        handler,
        liveness=IdleProbe(idle=60.0, probe=b"POW?;\n", attempts=3),
        time_func=clock.time,
        sleep_func=clock.sleep,
    )
    link.respond({b"POW?;\n": b"POW1\n"})
    await dev.start()
    try:
        await spin(lambda: link.sent.count(b"POW?;\n") >= 3)
        assert b"POW?;\n" in link.sent  # idle DID trigger probes
        assert link.connects == 1  # answered -> never declared dead
        assert dev.connected
        assert clock.now >= 180.0  # three virtual minutes, no real wait
    finally:
        await dev.stop()


async def test_idle_probe_declares_the_link_dead_after_attempts(
    link: FakeLink, handler: Recorder, clock: FakeClock
) -> None:
    dev = make_link(
        link,
        handler,
        liveness=IdleProbe(idle=60.0, probe=b"PING\n", attempts=2),
        time_func=clock.time,
        sleep_func=clock.sleep,
    )
    await dev.start()  # nothing ever answers
    try:
        await spin(lambda: link.connects >= 2)
        assert link.connects >= 2
        # Exactly `attempts` probes went out on the dead connection.
        assert link.writers[0].written.count(b"PING\n") == 2
    finally:
        await dev.stop()


async def test_any_rx_counts_as_liveness_not_just_a_probe_answer(
    link: FakeLink, handler: Recorder, clock: FakeClock
) -> None:
    """A chatty device proves itself alive with ordinary traffic; the watchdog
    must not reconnect a healthy link that simply ignores the probe."""
    dev = make_link(
        link,
        handler,
        liveness=IdleProbe(idle=60.0, probe=b"PING\n", attempts=2),
        time_func=clock.time,
        sleep_func=clock.sleep,
    )

    def chatter(data: bytes) -> None:
        if data == b"PING\n":
            link.rx(b"UNRELATED_REPORT\n")  # not an answer, but it IS traffic

    link.on_write = chatter
    await dev.start()
    try:
        await spin(lambda: clock.now >= 600.0)
        assert link.connects == 1
        assert dev.connected
    finally:
        await dev.stop()


async def test_passive_idle_probe_needs_no_frame_to_send(
    link: FakeLink, handler: Recorder, clock: FakeClock
) -> None:
    """probe=None: a device that reports continuously needs no poke, so
    silence alone counts down."""
    dev = make_link(
        link,
        handler,
        liveness=IdleProbe(idle=60.0, probe=None, attempts=2),
        time_func=clock.time,
        sleep_func=clock.sleep,
    )
    await dev.start()
    try:
        await spin(lambda: link.connects >= 2)
        assert link.connects >= 2
        assert link.writers[0].written == []  # nothing was ever sent
    finally:
        await dev.stop()


async def test_probe_write_failure_triggers_reconnect(
    link: FakeLink, handler: Recorder, clock: FakeClock
) -> None:
    dev = make_link(
        link,
        handler,
        liveness=IdleProbe(idle=60.0, probe=b"PING\n", attempts=3),
        time_func=clock.time,
        sleep_func=clock.sleep,
    )
    await dev.start()
    try:
        link.fail_writes()
        await spin(lambda: link.connects >= 2)
        assert link.connects >= 2
    finally:
        await dev.stop()


# ---- FailureCount -------------------------------------------------------


async def test_failure_count_trips_reconnect_after_n_timeouts(
    link: FakeLink, handler: Recorder
) -> None:
    """A device that emits nothing unsolicited cannot be watched passively:
    silence is its resting state, so an idle window would either never fire or
    fire constantly. Without this, a proxy whose connection dies hangs the
    dispatch task forever while consumers serve stale state indefinitely.
    Unanswered commands are the only available evidence."""
    dev = make_link(link, handler, liveness=FailureCount(consecutive=3))
    await dev.start()
    try:
        for _ in range(2):
            async with dev.exchange() as ex:
                await ex.send(b"Q\n")
                with pytest.raises(CommandTimeoutError):
                    await ex.next(timeout=0.02)
        assert link.connects == 1  # not yet: two is under the threshold

        async with dev.exchange() as ex:
            await ex.send(b"Q\n")
            with pytest.raises(CommandTimeoutError):
                await ex.next(timeout=0.02)

        for _ in range(400):
            if link.connects >= 2:
                break
            await asyncio.sleep(0.005)
        assert link.connects >= 2
    finally:
        await dev.stop()


async def test_a_success_resets_the_failure_count(
    link: FakeLink, handler: Recorder
) -> None:
    """ "Consecutive" means consecutive: an answered command clears the debt,
    including one for a different command."""
    dev = make_link(link, handler, liveness=FailureCount(consecutive=3))
    await dev.start()
    try:
        for _ in range(2):
            async with dev.exchange() as ex:
                await ex.send(b"QA\n")
                with pytest.raises(CommandTimeoutError):
                    await ex.next(timeout=0.02)

        link.respond({b"QB\n": b"OK\n"})  # a DIFFERENT command answers
        async with dev.exchange() as ex:
            await ex.send(b"QB\n")
            assert await ex.next(timeout=1.0) == b"OK"
        link.on_write = None

        # Two more failures: 2 + 2 = 4 total, but only 2 consecutive.
        for _ in range(2):
            async with dev.exchange() as ex:
                await ex.send(b"QA\n")
                with pytest.raises(CommandTimeoutError):
                    await ex.next(timeout=0.02)

        await asyncio.sleep(0.05)
        assert link.connects == 1
    finally:
        await dev.stop()


async def test_confirm_timeouts_also_count(link: FakeLink, handler: Recorder) -> None:
    dev = make_link(link, handler, liveness=FailureCount(consecutive=2))
    await dev.start()
    try:
        for _ in range(2):
            with pytest.raises(CommandTimeoutError):
                await dev.confirm(
                    nudge=b"Q\n",
                    match=lambda f: f.startswith(b"A"),
                    timeout=0.02,
                )
        for _ in range(400):
            if link.connects >= 2:
                break
            await asyncio.sleep(0.005)
        assert link.connects >= 2
    finally:
        await dev.stop()


async def test_confirm_retries_are_one_command_not_many(
    link: FakeLink, handler: Recorder
) -> None:
    """A confirm(retries=2) that eventually fails is ONE unanswered command,
    not three — otherwise a single standby power-on would trip the watchdog."""
    dev = make_link(link, handler, liveness=FailureCount(consecutive=2))
    await dev.start()
    try:
        with pytest.raises(CommandTimeoutError):
            await dev.confirm(
                nudge=b"Q\n",
                match=lambda f: f.startswith(b"A"),
                timeout=0.02,
                retries=2,
            )
        assert link.sent == [b"Q\n"] * 3
        await asyncio.sleep(0.05)
        assert link.connects == 1  # one failure, threshold is two
    finally:
        await dev.stop()


async def test_failure_count_resets_on_a_new_session(
    link: FakeLink, handler: Recorder
) -> None:
    dev = make_link(link, handler, liveness=FailureCount(consecutive=2))
    await dev.start()
    try:
        with pytest.raises(CommandTimeoutError):
            await dev.confirm(nudge=b"Q\n", match=lambda f: False, timeout=0.02)
        link.drop()
        for _ in range(400):
            if dev.connected and link.connects >= 2:
                break
            await asyncio.sleep(0.005)
        assert dev._consecutive_failures == 0
    finally:
        await dev.stop()


# ---- sweep --------------------------------------------------------------


async def test_sweep_sends_everything_then_waits_for_quiet(
    link: FakeLink, handler: Recorder, clock: FakeClock
) -> None:
    dev = make_link(link, handler, time_func=clock.time, sleep_func=clock.sleep)
    frames = [b"POW?;\n", b"VOL?;\n", b"MUT?;\n"]
    link.respond({f: f.replace(b"?", b"1") for f in frames})
    await dev.start()
    try:
        await dev.sweep(frames, quiet=0.3, timeout=5.0)
        assert link.sent == frames
        # Replies went through on_frame normally — a sweep has no per-frame
        # success or failure, only events that do or do not arrive.
        assert len(handler.frames) == 3
    finally:
        await dev.stop()


async def test_sweep_keeps_waiting_while_replies_still_arrive(
    link: FakeLink, handler: Recorder, clock: FakeClock
) -> None:
    """Waiting for silence rather than sleeping a fixed amount is what lets it
    adapt to a slow device."""
    dev = make_link(link, handler, time_func=clock.time, sleep_func=clock.sleep)
    await dev.start()
    try:
        remaining = [b"A\n", b"B\n", b"C\n"]

        async def dribble() -> None:
            """One reply per quiet window, so the sweep has to wait out
            several of them to collect all three."""
            fed_at = 0.0
            while remaining:
                await asyncio.sleep(0)
                if clock.now > fed_at:
                    fed_at = clock.now
                    link.rx(remaining.pop(0))

        drip = asyncio.ensure_future(dribble())
        await dev.sweep([b"Q\n"], quiet=0.3, timeout=60.0)
        await drip

        assert len(handler.frames) == 3
        # Four windows: three that saw a reply, then one silent one.
        assert clock.now == pytest.approx(1.2)
    finally:
        await dev.stop()


async def test_sweep_gives_up_at_its_timeout(
    link: FakeLink, handler: Recorder, clock: FakeClock
) -> None:
    """A device that never stops talking must not hold the sweep forever."""
    dev = make_link(link, handler, time_func=clock.time, sleep_func=clock.sleep)
    await dev.start()
    try:
        chatty = True

        async def never_shut_up() -> None:
            while chatty:
                link.rx(b"CHATTER\n")
                await asyncio.sleep(0)

        noise = asyncio.ensure_future(never_shut_up())
        await dev.sweep([b"Q\n"], quiet=0.3, timeout=3.0)
        chatty = False
        await noise

        assert clock.now >= 3.0
        assert clock.now < 10.0  # bounded by timeout, not running away
    finally:
        await dev.stop()


# ---- pacing on the link -------------------------------------------------


async def test_sends_are_spaced_by_the_selected_interval(
    link: FakeLink, handler: Recorder, clock: FakeClock
) -> None:
    from serialkit import Pacing

    dev = make_link(
        link,
        handler,
        pacing=Pacing(min_interval=0.1, per_command={b"PW": 0.5}),
        time_func=clock.time,
        sleep_func=clock.sleep,
    )
    await dev.start()
    try:
        await dev.send(b"MV50\n")
        assert clock.now == 0.0  # the first send is immediate
        await dev.send(b"PWON\n")
        assert clock.now == pytest.approx(0.1)  # MV's settle interval
        await dev.send(b"MV51\n")
        assert clock.now == pytest.approx(0.6)  # PW's longer settle interval
    finally:
        await dev.stop()


async def test_a_chained_write_is_one_pacing_unit(
    link: FakeLink, handler: Recorder, clock: FakeClock
) -> None:
    from serialkit import Pacing

    dev = make_link(
        link,
        handler,
        pacing=Pacing(min_interval=0.1),
        time_func=clock.time,
        sleep_func=clock.sleep,
    )
    await dev.start()
    try:
        await dev.send(b"POW?;VOL?;MUT?\n")
        await dev.send(b"SRC?;AUD?\n")
        assert clock.now == pytest.approx(0.1)  # one interval, not one each
        assert clock.sleeps == [pytest.approx(0.1)]
    finally:
        await dev.stop()


async def test_pacing_can_be_shared_between_links(
    handler: Recorder, clock: FakeClock
) -> None:
    """A frozen Pacing carries no lock and no timestamp, so the pacing debt is
    per-link even when the policy object is shared."""
    from serialkit import Pacing

    shared = Pacing(min_interval=0.1)
    link_a, link_b = FakeLink(), FakeLink()
    dev_a = make_link(
        link_a, handler, pacing=shared, time_func=clock.time, sleep_func=clock.sleep
    )
    dev_b = make_link(
        link_b, Recorder(), pacing=shared, time_func=clock.time, sleep_func=clock.sleep
    )
    await dev_a.start()
    await dev_b.start()
    try:
        await dev_a.send(b"A\n")
        # B's first send is not delayed by A's: the pacing debt is per-link.
        await dev_b.send(b"B\n")
        assert clock.now == 0.0
    finally:
        await dev_a.stop()
        await dev_b.stop()
