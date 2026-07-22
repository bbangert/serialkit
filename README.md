# serialkit

[![Test](https://github.com/bbangert/serialkit/actions/workflows/test.yml/badge.svg)](https://github.com/bbangert/serialkit/actions/workflows/test.yml)
[![PyPI](https://img.shields.io/pypi/v/serial-toolkit.svg)](https://pypi.org/project/serial-toolkit/)
[![Python](https://img.shields.io/pypi/pyversions/serial-toolkit.svg)](https://pypi.org/project/serial-toolkit/)

Asyncio robustness toolkit for RS232 device drivers, built on
[serialx](https://github.com/puddly/serialx).

Serial device libraries keep hand-rolling the same hard parts — framing a byte
stream, correlating responses to requests, pacing writes, running a read loop,
a liveness watchdog, and reconnect. serialkit provides those once, correctly,
behind an `asyncio.Protocol`-flavoured callback API. Subclass `SerialDevice`,
declare a little config, and override a few callbacks; or drop to the
primitives (`PendingTracker`, the framers, `Pacing`) when a protocol needs
bespoke handling.

## Installation

```bash
pip install serial-toolkit

# To talk to a device over an ESPHome serial proxy:
pip install 'serial-toolkit[esphome]'
```

The distribution is named `serial-toolkit` on PyPI; the import package is
`serialkit` (`import serialkit`). Requires Python 3.14+.

## Concepts

- **One dispatch task per connection.** It reads the transport, frames each
  chunk, and calls your sync `on_frame` once per frame. A crash in `on_frame`
  is recorded and swallowed — it never kills the loop or the next frame.
- **Correlation is matcher-based, never positional.** You register what a
  response looks like (`match_prefix(b"POW")`); the kit resolves the oldest
  in-flight request whose matcher accepts an incoming frame. Positional (FIFO)
  correlation is the classic desync: one dropped answer shifts every reply.
- **The transport is injected.** You give `SerialDevice` an async `connect`
  factory returning a duck-typed `(reader, writer)`. In production that wraps
  `serialx.open_serial_connection`; in tests it's `serialkit.testing.FakeLink`.
- **State is rebuilt per connection.** On reconnect the kit calls `make_state`
  again; nothing survives across a drop, so subscribers never see stale fields.

## Minimal driver

```python
from serialx import open_serial_connection

from serialkit import DelimiterFramer, SerialDevice, match_prefix


class MyTv(SerialDevice[dict]):
    framer_factory = staticmethod(lambda: DelimiterFramer(b"\r"))
    max_in_flight = 1                       # one command owed a reply at a time

    @classmethod
    def open(cls, url: str) -> "MyTv":
        async def connect():
            return await open_serial_connection(url=url, baudrate=9600)
        return cls(connect)

    def make_state(self) -> dict:
        return {}

    async def on_connect(self) -> None:
        await self.query_power()             # frames are already flowing here

    def on_frame(self, frame: bytes) -> None:
        if frame.startswith(b"POW"):
            self.state["power"] = frame[3:] == b"1"
            self.notify()
        if not self.pending.feed(frame):
            ...  # an unsolicited event: update state + self.notify()

    async def query_power(self) -> bytes:
        return await self.request(b"POW?\r", match_prefix(b"POW"))
```

```python
tv = MyTv.open("/dev/ttyUSB0")
await tv.start()
tv.subscribe(lambda state: print("state:", state))
await tv.query_power()
await tv.stop()
```

## The callback contract

### Config (class attributes)

| Attribute | Default | Meaning |
| --- | --- | --- |
| `framer_factory` | *(required)* | A zero-arg callable **or** a `Framer` instance used as a prototype. A fresh framer is built per connection. A plain-function factory is read off the class, so `staticmethod` is optional but conventional. |
| `pacing` | `None` | A `Pacing` policy; `None` means no spacing. Declared as a class attribute, it is cloned per instance (its lock and clock are never shared). |
| `probe` | `None` | A `ProbeSpec` opt-in liveness watchdog; `None` means no watchdog. |
| `backoff` | `Backoff()` | Reconnect delay policy (`initial * factor**tries`, capped). |
| `max_in_flight` | `None` | Slot gate. `1` refuses to even write a second command while the first is owed a reply. `None` is unlimited. |
| `request_timeout` | `3.0` | Default per-request deadline (seconds). |

### Lifecycle callbacks (override)

- `make_state(self) -> S` — build fresh state for a new connection.
- `async on_connect(self) -> None` — handshake / verify / initial query. The
  dispatch task is already live, so `await self.request(...)` works here and is
  the idiomatic handshake.
- `on_frame(self, frame: bytes) -> None` — **sync**, runs on the dispatch task,
  one call per frame in order. You decide the ordering of state mutation vs
  `self.pending.feed(frame)`.
- `on_disconnect(self, exc: Exception | None) -> None` — optional; runs before
  a reconnect (`exc` set) and on `stop()` (`exc=None`).
- `copy_state(self, state: S) -> S` — snapshot for subscribers; defaults to
  `copy.deepcopy`. Override with a cheaper `.copy()` for dataclasses.

### Facilities (call / read)

- `state: S` — the live state object; mutate it from callbacks or command
  methods.
- `pending: PendingTracker` — `feed(frame)` resolves a matching request
  (returns `False` if unsolicited); `reject_matched(key, exc, *, all=)` and
  `reject_oldest(exc)` fail requests on an error frame.
- `async request(frame, matcher, *, timeout=None, pace=None) -> bytes` —
  slot-gated, paced request; returns the correlated response frame.
- `async send(frame, *, pace=None) -> None` — paced write, no reply expected.
- `notify() -> None` — request a coalesced snapshot to subscribers (at most one
  per dispatch turn). Call it after you change `state`.
- `batch()` — context manager coalescing a burst of awaited requests into a
  single notification (`with self.batch(): await self.query_all()`).
- `subscribe(cb) -> unsubscribe` — `cb` receives an `S` snapshot, or `None` on
  disconnect.
- `async start()` / `async stop()` — open (and supervise) / tear down.
- `connected: bool`.

### Pinned semantics (the sharp edges)

- **A request caller resumes strictly after the dispatch turn containing its
  response.** Mutating `state` right after `await self.request(...)` lands on
  top of everything that turn dispatched — safe at single-loop granularity.
- **A paced write is abandoned if its request already completed** (timeout /
  disconnect) while it was queued behind pacing. The kit never puts an
  untracked command on the wire — that is the desync `max_in_flight` prevents.
- **`request()`/`send()` raise `ConnectionLostError` immediately when not
  connected** (before `start()`, during backoff, after `stop()`). No queueing
  across reconnects.
- **`notify(None)` (disconnect) discards any unflushed snapshot** — subscribers
  never see a stale snapshot after `None`.
- **A sequential burst of awaited requests notifies once per response** —
  dispatch-turn coalescing only merges answers that arrive in one chunk. Use
  `batch()` to collapse a sequential burst.

## Connection lifecycle

```
start()
  └─ connect ─ make_state ─ [dispatch task up] ─ on_connect ─ notify ─┐
                                                                       │
        ┌──────────────────── supervised session ────────────────────┘
        │   read → framer.feed → on_frame per frame → coalesced notify
        │   request()/send() from command methods
        │
        ├─ read error / EOF / probe timeout ─┐
        │                                     ▼
        │   fail_all(pending) ─ on_disconnect(exc) ─ notify(None)
        │        └─ backoff(1.8**tries, cap 60s) ─ fresh framer + make_state
        │                        └─ on_connect ─ notify ─┐
        │                                                 │
        └─────────────────── loop ◄───────────────────────┘

stop()  → fail_all(pending) ─ on_disconnect(None) ─ notify(None) ─ (no reconnect)
```

## Migrating a hand-rolled driver (skeleton)

The five device libraries this toolkit serves each hand-roll the machinery
below. Migration replaces it with kit facilities:

| Hand-rolled today | Replace with |
| --- | --- |
| Read loop (`while: reader.read` + buffer split) | `framer_factory` + `on_frame` |
| FIFO `pending.pop(0)` correlation | `pending.feed(frame)` + matchers |
| `asyncio.sleep(...)` between sends | `pacing = Pacing(min_interval=...)` |
| Manual reconnect / backoff task | kit reconnect loop (on by default) |
| Bespoke keepalive / heartbeat | `probe = ProbeSpec(...)` (opt-in) |
| `connect()` + `query_state()` wiring | `on_connect` owns `query_state` |
| `subscribe`/notify bookkeeping | `subscribe` + `notify` + `batch` |
| Custom exception hierarchy | subclass `ProtocolError` |

Steps: (1) subclass `SerialDevice[YourState]`; (2) move framing into a
`Framer`; (3) turn each `set_*`/`query_*` into `await self.request(...)` +
state mutation + `notify()`; (4) delete the read loop, the sleeps, and the
reconnect task; (5) point the HA coordinator's `except` at the kit error types.
See `sony-tv-rs232` for the first worked migration.

## When to drop to primitives

`SerialDevice` is the right default. Reach past it to the primitives when:

- The protocol can't be expressed by the general framers (e.g. sony's
  checksum-discriminated short-ack vs long frame) — write your own `Framer`
  (it's just `feed(data) -> list[bytes]` + `reset()`), but still hand it to
  `SerialDevice`.
- You need correlation or pacing outside a connection lifecycle (a one-shot
  probe tool, a discovery scan) — use `PendingTracker` / `Pacing` directly.
- You are embedding serial handling into an existing runtime that already owns
  the read loop — use the framer + tracker and skip the dispatch task.

The primitives (`PendingTracker`, `DelimiterFramer` / `RegexResyncFramer` /
`LengthPrefixedFramer`, `Pacing`) are public and independently usable.

## Testing

`serialkit.testing` ships the transport doubles so drivers never touch real
hardware:

```python
from serialkit.testing import FakeLink

link = FakeLink()
link.respond({b"POW?\r": b"POW1\r"})   # script a device answer
dev = MyTv(link.connect)
await dev.start()
assert link.sent == [b"POW?\r"]        # assert what was written
link.garble()                          # inject a corrupt burst (desync test)
link.drop()                            # EOF -> exercise reconnect
```

`FakeClock` makes `Pacing` deterministic (`sleep` advances virtual time).

## Development

```bash
uv run --python 3.14 pytest
uvx --python 3.14 mypy@latest --strict src/
uvx ruff check
```

## License

MIT
