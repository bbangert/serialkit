# serialkit

Asyncio robustness toolkit for RS232 device drivers, built on
[serialx](https://github.com/puddly/serialx).

Serial device libraries keep hand-rolling the same hard parts — framing a byte
stream, correlating responses to requests, pacing writes, a read loop, a
liveness watchdog, and reconnect. serialkit provides those once, correctly,
behind an `asyncio.Protocol`-flavoured callback API. Subclass `SerialDevice`,
declare a little config, and override a few callbacks; or drop to the
primitives (`PendingTracker`, the framers, `Pacing`) when a protocol needs
bespoke handling.

> **Status:** alpha. The full callback contract, lifecycle diagram, and
> migration guide land with the 0.1.0 documentation pass. This is a stub.

## Installation

```bash
pip install serialkit

# To talk to a device over an ESPHome serial proxy:
pip install 'serialkit[esphome]'
```

Requires Python 3.14+.

## Minimal driver

```python
from serialkit import DelimiterFramer, SerialDevice, match_prefix


class MyDevice(SerialDevice[dict]):
    framer_factory = staticmethod(lambda: DelimiterFramer(b"\r"))

    def make_state(self) -> dict:
        return {}

    def on_frame(self, frame: bytes) -> None:
        if not self.pending.feed(frame):
            ...  # unsolicited event: update state + self.notify()

    async def query_power(self) -> bytes:
        return await self.request(b"POW?\r", match_prefix(b"POW"))
```

## Development

```bash
uv run --python 3.14 pytest
uvx --python 3.14 mypy@latest --strict src/
uvx ruff check
```

## License

MIT
