# serialkit

Wire mechanics for RS232 device drivers. Not a device library — the substrate
each `<device>-rs232` library builds on for framing, the read loop, pacing,
liveness, reconnect, and the anchoring that keeps a late reply from being read
as the next command's answer.

**serialkit is generic.** It names no manufacturer, model, or command
vocabulary anywhere — not in code, docstrings, tests, or docs. Where a real
device motivated a design decision, record the *device behaviour* that forced
it ("a device asleep in standby consumes the first frame waking up"), never the
brand. Device-specific knowledge belongs one layer up.

Published to PyPI as **`serial-toolkit`** (the name `serialkit` is blocked
there); the import package is `serialkit`.

## The three layers

| Layer | Owns | Does not own |
| --- | --- | --- |
| **serialkit** | Framing, pacing, exclusivity, sequence anchoring, dispatch, reconnect, liveness. Tells the caller when a connection opens and drops. | Any knowledge of a device, a command, or a response. No state. |
| **`<device>-rs232`** | The device: what commands go in, what comes out, how a frame decodes into a typed event, the full re-query on connect, and the device model. | Home Assistant concepts, entity mapping. |
| **`<device>-rs232-hass`** | The projection of the device model onto HA: entities, `_attr_*`, availability, unit conversion. | Framing, pacing, reconnect, protocol encoding, or a second copy of the model. |

**Mechanism vs policy is the sharp edge between layers 1 and 2.** The kit
provides *waiting for a frame matching a predicate*, correctly anchored, plus
exclusivity, pacing, and the retry loop. The driver supplies the predicate, the
frames, the retry count, and the interpretation. "This device needs
`retries=1` because its standby MCU eats the first frame" is device knowledge;
the loop implementing it is not.

## Project structure

```
src/serialkit/
  __init__.py    -- the public surface (14 names)
  errors.py      -- SerialKitError -> ConnectionLostError / CommandTimeoutError
                    / ProtocolError / ResyncError (drivers subclass ProtocolError)
  framing.py     -- Framer protocol + DelimiterFramer / RegexResyncFramer
                    (bespoke protocols ship their own Framer)
  pacing.py      -- Pacing: a frozen settle-after policy, per-command overrides
  link.py        -- SerialLink runtime + DeviceHandler + Backoff + IdleProbe
                    + FailureCount
  waiting.py     -- the frame-waiter registry, the RX counter, and Exchange
  testing.py     -- FakeLink / FakeClock / FakeReader / FakeWriter

tests/
  conftest.py               -- fixtures + the recording Recorder handler
  test_framing.py           -- framer split/NUL/oversize/reset scenarios
  test_framing_vectors.py   -- byte vectors shaped like the real protocols
  test_pacing.py            -- interval selection as a pure function
  test_testing_doubles.py   -- the fault shapes the doubles must reproduce
  test_link_runtime.py      -- start/dispatch/turns/reconnect/teardown pins
  test_waiting.py           -- arming, routing order, RX anchoring
  test_liveness.py          -- both liveness shapes, sweep, link-level pacing
```

## Architecture

- **The driver owns a link and implements `DeviceHandler`.** That keeps the
  kit out of the driver's namespace, makes per-instance config (a
  model-dependent baud rate) ordinary, and lets each library name its own
  public API.
- **`SerialLink` is not generic and holds no device data.** It never sees a
  command, a response, or a state object. The device model belongs to the
  driver: the event loop is single-threaded, so there is no interleaving for
  snapshots to guard, and diffing to detect change would rediscover at copy
  time what the driver already knew at mutation time.
- **Single dispatch task per connection.** It reads the transport, frames each
  chunk, calls the sync `on_frame` per frame (exception hardened), then
  `on_turn` once for the chunk. `on_turn` is the one thing a driver cannot
  compute for itself, because only the kit knows where a chunk's frames stop —
  it is the coalescing point where a driver flushes queued events.
- **Transport is injected** as an async `connect` factory returning a
  duck-typed `(reader, writer)` pair, so the runtime is testable without real
  hardware and works with any `serialx` URL (local, `socket://`, `esphome://`).
- **Kit-owned reconnect loop**, and `on_connect` runs on **every** connection
  with frames already flowing. That is where the driver re-queries: staleness
  after a reconnect is a protocol problem solved by asking the device again,
  not a state-lifecycle problem.
- **Liveness is opt-in and comes in two shapes**, because the device classes
  differ. A publisher going quiet is evidence (`IdleProbe`); a transactional
  device is silent at rest, so only unanswered commands are (`FailureCount`).

## Waiting for a frame

Two shapes, matching how the device talks. `exchange()` is request/reply for a
device where every frame is caused by something sent; it holds the wire so the
reply to the outstanding command is unambiguous, and anchors on arrival order
because that is the discriminator these protocols reliably give you. `expect()`
is for a device that reports state on its own schedule, where an arriving frame
often answers nothing in particular.

- `expect(match, timeout)` arms a waiter **at call time**, so it can be armed
  before the send and a same-chunk reply is never lost. It **observes without
  consuming**: a matching frame still reaches `on_frame`, because the frame
  confirming a power-on is also the state report the driver must apply.
- `confirm(nudge=…, match=…, timeout=…, retries=…)` arms, sends, waits,
  retries. It resolves only on a frame the device actually sent, since one that
  has not arrived yet cannot already satisfy the predicate.
- `exchange()` holds the wire exclusively and **claims** the frames it reads,
  for a device whose replies are only decodable against the outstanding
  command.
- `sweep(frames)` sends everything under pacing then waits for silence — the
  bulk refresh for a publisher. No per-frame success or failure.

Frame routing order, per arriving frame:

1. an open exchange that is expecting a reply **claims** it;
2. otherwise every armed `expect` waiter whose predicate matches resolves;
3. `on_frame(frame)` runs regardless of step 2.

## Load-bearing contracts (do not regress)

- **An exchange reply is anchored by arrival order, recorded at write time —
  after pacing, not before it.** This is the structural fix for the classic
  serial desync: a late reply to a previous, timed-out exchange arriving while
  the next command waits its pacing turn lands behind the anchor and is
  discarded. A content matcher cannot express this, because when replies carry
  no identifier the two frames are byte-identical.
- **`expect()` observes, `exchange()` consumes.** Getting this backwards
  silently drops state updates.
- **Nothing queues across a reconnect.** `send`/`sweep`/`confirm`/`exchange`
  fail immediately when not connected; a volume command delivered 60 seconds
  late is wrong for RS232.
- **A write is abandoned if the session changed while it was queued behind
  pacing** — a stale frame must never land on a new session's writer.
- **Reconnect fails all in-flight waits before `on_disconnect`**, or the next
  exchange deadlocks on the wire lock.
- **The runtime owns framer reset**, never the framer, and still routes the
  frames completed before a `ResyncError`.
- **`on_turn` fires once per chunk that produced at least one frame**, and not
  at all for a chunk that produced none.
- **The framer is a prototype instance**, deep-copied and reset per connection.
- **The clock seam covers the sleeps the kit initiates** — pacing, backoff, the
  liveness idle window, the sweep quiet window. It deliberately does *not*
  cover `expect`/`confirm`/`Exchange.next` timeouts: those are deadlines on
  external events, and a virtual clock whose `sleep` returns immediately would
  win every race against a real future and fire every timeout instantly.

Escape hatch: when `SerialLink` doesn't fit (a one-shot tool, a runtime that
already owns the read loop), drop to the primitives — the framers, `Pacing`,
`WaitRegistry` — directly.

## Testing

- `pytest` with `pytest-asyncio`, `asyncio_mode = "auto"`; `RuntimeWarning` is
  promoted to an error so un-awaited futures/coroutines fail the suite.
- No test requires real hardware. `FakeLink` is an injected connect factory
  that reproduces the transport fault shapes as they actually occur: EOF for a
  socket FIN, a raised `OSError` for a serial unplug, and a *silent* write
  failure where `write()` and `drain()` both return normally and the frame
  simply never goes out.
- `FakeClock` makes a 60-second idle window cost no real time.
- Run under the target Python (PEP 758 makes some 3.13 mypy/compile findings
  false positives):

  ```bash
  uv run --python 3.14 pytest
  uvx --python 3.14 mypy@latest --strict src/
  uvx ruff check && uvx ruff format --check
  ```
