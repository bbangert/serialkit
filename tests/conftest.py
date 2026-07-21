"""Test harness. The transport doubles now live in ``serialkit.testing`` (the
reusable module drivers import); this conftest re-exports them for the local
tests and adds the baseline DictDevice fixture."""

from __future__ import annotations

import pytest

from serialkit import Backoff, DelimiterFramer, SerialDevice
from serialkit.testing import FakeClock, FakeLink, FakeReader, FakeWriter

__all__ = ["DictDevice", "FakeClock", "FakeLink", "FakeReader", "FakeWriter"]


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
