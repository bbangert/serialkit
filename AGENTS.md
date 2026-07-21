# serialkit

Asyncio robustness toolkit for RS232 device drivers. Not a device library — a
reusable substrate the device libraries (`sony-tv-rs232`, `denon-rs232`,
`lg-rs232-tv`, `anthem-rs232`) build on so each one stops hand-rolling framing,
read loops, watchdogs, reconnect, and request/response correlation.

## Project structure

```
src/serialkit/
  __init__.py    -- Re-exports the public API
  errors.py      -- SerialKitError -> ConnectionLostError / CommandTimeoutError
                    / ProtocolError / ResyncError (drivers subclass ProtocolError)
  framing.py     -- Framer protocol + DelimiterFramer / RegexResyncFramer /
                    LengthPrefixedFramer (bespoke protocols ship their own Framer)
  correlate.py   -- PendingTracker: matcher-based (never FIFO) request/response
                    correlation with a max_in_flight slot gate
  pacing.py      -- Pacing: settle-after minimum spacing, per-command overrides
  device.py      -- SerialDevice[S] runtime + Backoff + ProbeSpec

tests/
  conftest.py            -- FakeClock, FakeReader/Writer, FakeLink, DictDevice
  test_framing.py        -- framer split/NUL/oversize/reset scenarios
  test_pacing.py         -- deterministic fake-clock pacing scenarios
  test_teardown.py       -- fail_all + slot release + cancellation
  test_desync.py         -- sony desync regression (gated vs FIFO contrast)
  test_errors_routing.py -- gen1/gen2 error-frame rejection in on_frame
  test_device_runtime.py -- handshake, reconnect-rebuild, sony ordering,
                            burst notify coalescing, probe watchdog, stop()
```

## Architecture

- **Single dispatch task per connection.** It reads the transport, frames each
  chunk, and calls the sync `on_frame` callback per frame (exception-hardened —
  a driver crash on one frame never kills the loop or the next frame). At most
  one coalesced subscriber notification is delivered per dispatch turn.
- **Callback surface, OTP-inspired internals.** Drivers subclass
  `SerialDevice[S]`, declare config as class attributes (`framer_factory`,
  `pacing`, `probe`, `backoff`, `max_in_flight`, `request_timeout`), and
  override `make_state` / `on_connect` / `on_frame` / `on_disconnect` /
  `copy_state`.
- **Transport is injected** as an async `connect` factory returning a
  duck-typed `(reader, writer)` pair, so the runtime is testable without real
  hardware and works with any `serialx` URL (local, `socket://`, `esphome://`).
- **Kit-owned reconnect loop.** On read error / EOF / probe failure: `fail_all`
  pending → `on_disconnect(exc)` → `notify(None)` → backoff → fresh framer +
  fresh `make_state()` state → `on_connect` → `notify`. State is rebuilt per
  connection; nothing is preserved across a reconnect.
- **Watchdog is opt-in** via the `probe` class attribute, using jitter-immune
  idle-window checkpoints (any RX in the window = alive), never a clock delta.

## Load-bearing contracts (do not regress)

- **Correlation is matcher-based, never positional.** FIFO correlation is the
  sony production desync: a dropped/garbled answer shifts every later response.
- **`max_in_flight=1` gates write AND response-wait** — a second command isn't
  even written while the first is owed a reply.
- **Timeout starts at slot acquisition, and a paced write is abandoned if its
  pending completed while queued behind pacing.** Emitting a frame whose
  pending is gone puts an untracked command on the wire — the desync again.
- **A request caller resumes strictly after the dispatch turn containing its
  response frame** (single-loop sync granularity makes caller-task state
  mutation safe).
- **`notify(None)` discards any dirty-but-unflushed snapshot** so a stale
  snapshot can never follow `None`.
- **`framer_factory` must be a `staticmethod`.**

Escape hatch: when `SerialDevice` doesn't fit, drop to the primitives
(`PendingTracker`, a `Framer`, `Pacing`) directly.

## Testing

- `pytest` with `pytest-asyncio`, `asyncio_mode = "auto"`; `RuntimeWarning` is
  promoted to an error so un-awaited futures/coroutines fail the suite.
- No test requires real hardware: `FakeLink` is an injected connect factory
  with scriptable responses; `FakeClock` makes pacing deterministic.
- Run under the target Python (PEP 758 makes some 3.13 mypy/compile findings
  false positives): `uv run --python 3.14 pytest`,
  `uvx --python 3.14 mypy@latest --strict src/`, `uvx ruff check`.
