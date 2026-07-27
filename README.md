# serialkit

[![Test](https://github.com/bbangert/serialkit/actions/workflows/test.yml/badge.svg)](https://github.com/bbangert/serialkit/actions/workflows/test.yml)
[![PyPI](https://img.shields.io/pypi/v/serial-toolkit.svg)](https://pypi.org/project/serial-toolkit/)
[![Python](https://img.shields.io/pypi/pyversions/serial-toolkit.svg)](https://pypi.org/project/serial-toolkit/)

Wire mechanics for RS232 device drivers, built on
[serialx](https://github.com/puddly/serialx).

serialkit handles framing a byte stream, pacing writes, the read loop, liveness,
reconnect, and the anchoring that keeps a late reply from being read as the next
command's answer — behind an `asyncio.Protocol`-flavoured callback API.

Your driver owns a `SerialLink` and implements `DeviceHandler`, so your library
keeps its own public API.

## Installation

```bash
pip install serial-toolkit

# To talk to a device over an ESPHome serial proxy:
pip install 'serial-toolkit[esphome]'
```

The distribution is named `serial-toolkit` on PyPI; the import package is
`serialkit` (`import serialkit`). Requires Python 3.14+.

## Scope

serialkit owns the wire: framing, pacing, exclusivity, sequence anchoring,
dispatch, reconnect, and liveness.

It knows nothing about any device: no commands, no responses, no state. Your
device model lives in your library as an ordinary attribute, with no kit
contract attached to it.

## Concepts

- **One dispatch task per connection.** It reads the transport, frames each
  chunk, and calls your sync `on_frame` once per frame in order. A crash in
  `on_frame` is recorded in `frame_errors`; the loop and the rest of the chunk
  carry on.
- **`on_turn()` marks the end of a read chunk**, once every frame in it has
  reached `on_frame`. Flush your queued events here and a burst of frames
  becomes a single notification.
- **`on_connect()` runs with frames already flowing**, so `send`, `sweep`,
  `expect` and `exchange` all work inside it. Put your full re-query here and
  every reconnect refreshes your state for free.
- **Two ways to wait, matching how your device talks.** `exchange()` is
  request/reply for a device where every frame answers something you sent: it
  holds the wire, sends, and returns the next frame to arrive. `expect()` waits
  for a frame matching a predicate, for a device that reports state on its own
  schedule — where a frame often answers nothing in particular.
- **The transport is injected.** You give `SerialLink` an async `connect`
  factory returning a duck-typed `(reader, writer)`. In production that wraps
  `serialx.open_serial_connection`; in tests it's `serialkit.testing.FakeLink`.

## Minimal driver

```python
from serialx import open_serial_connection

from serialkit import DelimiterFramer, IdleProbe, Pacing, SerialLink

QUERY_FRAMES = [b"POW?;", b"VOL?;", b"MUT?;"]


class MyReceiver:
    def __init__(self, port: str) -> None:
        self.state = ReceiverState()
        self.link = SerialLink(
            connect=lambda: open_serial_connection(url=port, baudrate=115200),
            framer=DelimiterFramer(b";", strip=b"\x00"),
            handler=self,
            pacing=Pacing(min_interval=0.03),
            liveness=IdleProbe(idle=60.0, probe=b"POW?;", attempts=3),
        )
        self._events: list[Event] = []

    # ---- the DeviceHandler callbacks ----

    def on_frame(self, frame: bytes) -> None:
        event = parse_event(frame)  # a pure function you can unit-test
        if event is not None:
            apply_event(self.state, event)  # ...and so is this
            self._events.append(event)

    def on_turn(self) -> None:
        events, self._events = self._events, []
        for subscriber in self._subscribers:
            subscriber(events)  # one callback per turn, batched

    async def on_connect(self) -> None:
        await self.link.send(b"ECH1;")  # enable auto-reports
        await self.link.sweep(QUERY_FRAMES)  # the full refresh

    def on_disconnect(self, exc: Exception | None) -> None:
        for subscriber in self._subscribers:
            subscriber([Disconnected(reason=exc)])  # consumers go unavailable

    # ---- your public API ----

    async def set_volume(self, db: float) -> None:
        await self.link.send(f"VOL{db:g};".encode())

    async def power_on(self) -> None:
        # Send, wait ~1s, send again: a standby MCU consumes the first frame
        # waking up, so the second one is what acts.
        await self.link.confirm(
            nudge=b"POW1;",
            match=lambda f: f.startswith(b"POW"),
            timeout=1.0,
            retries=1,
        )
```

`on_turn` and `on_disconnect` are optional — omit them if you don't need them.

## Waiting for a frame

```python
# Arm BEFORE the send, so a reply in the same read chunk isn't missed.
waiter = link.expect(is_power_report, timeout=1.0)
await link.send(b"POW1;")
frame = await waiter
```

`expect()` **observes without consuming**: the frame still reaches `on_frame`,
because the frame that confirms a power-on is also the state report you must
apply. `confirm()` is the wrapper that arms, sends, waits, and retries.

For a device whose replies carry no identifier and are only decodable against
the outstanding command, hold the wire instead:

```python
async with link.exchange() as ex:
    await ex.send(encode_query(fn))
    frame = await ex.next(timeout=2.0)
```

An exchange **claims** the frames it reads, so they do not reach `on_frame`.
Its reply is anchored by arrival order, recorded at write time: a late reply to
a previous, timed-out exchange lands behind the anchor and is discarded rather
than being read as this one's answer.

Frame routing order, per arriving frame:

1. an open exchange that is expecting a reply **claims** it;
2. otherwise every armed `expect` waiter whose predicate matches resolves;
3. `on_frame(frame)` runs regardless of step 2.

## Liveness

Two shapes, matching how the device behaves when it is healthy:

```python
IdleProbe(idle=60.0, probe=b"POW?;", attempts=3)  # silence is evidence
FailureCount(consecutive=3)  # unanswered commands are
```

A device that reports state spontaneously goes quiet when the link dies, so an
idle window catches it. Set `probe=None` for one chatty enough that silence
alone is the signal.

A device that speaks only when spoken to is silent at rest, so its unanswered
commands are the evidence instead. This is what notices a proxy whose
connection died while the port still looks open.

Both judge liveness by idle-window checkpoints — was there any RX during this
window? — which is immune to scheduling jitter.

## Sharp edges

- **A command either goes out now or raises `ConnectionLostError`** — before
  `start()`, during backoff, after `stop()`. Commands are never buffered for a
  later connection, because a volume change delivered 60 seconds late is the
  wrong answer.
- **Pacing is settle-after.** The interval selected for a frame is the minimum
  delay *after it is sent*. A `;`-chained command written once passes the send
  path once and is one pacing unit.
- **A raise from `on_connect` fails the connection** — it propagates out of
  `start()` on the first attempt and triggers backoff-and-retry on a reconnect.
- **The framer is a prototype**, deep-copied and reset per connection, so no
  connection inherits another's residual buffer.
- **`sweep()` reports completion, not per-frame results.** An unanswered nudge
  is a field that never gets an event.

## Testing

`serialkit.testing` ships the doubles, so nothing needs real hardware:

```python
link = FakeLink()
link.respond({b"POW?\r": b"POW1\r"})
dev = MyDevice(link.connect)
await dev.start()
```

The fault shapes match what real transports do, so a recovery path is exercised
in the form it actually takes:

| | |
| --- | --- |
| `drop()` | EOF (`b""`) — the socket-family graceful FIN |
| `drop(abrupt=True)` | `read()` raises `OSError(EIO)` — a serial unplug |
| `fail_writes(silent=True)` | writes accepted and discarded, reproducing the fd transport where a failed `os.write` returns normally |
| `hang_connect()` | a connect that never resolves, for `connect_timeout` |

`FakeClock` drives the `time_func`/`sleep_func` seam, so a 60-second idle
window costs no real time. The seam covers the sleeps the kit *initiates* —
pacing, backoff, the liveness window, the sweep quiet window — which are the
expensive ones.

`expect`/`confirm`/`Exchange.next` timeouts stay on loop time, which keeps them
accurate deadlines on external events: a clock whose `sleep` returns
immediately would win every race against a real future and fire every timeout
instantly. They are short by nature, so tests pass small values.

## License

MIT
