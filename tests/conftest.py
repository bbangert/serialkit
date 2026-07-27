"""Shared fixtures. The transport doubles live in ``serialkit.testing`` (the
reusable module drivers import); this conftest wires them up as fixtures and
adds the recording ``DeviceHandler`` the link tests dispatch into."""

from __future__ import annotations

import pytest

from serialkit.testing import FakeClock, FakeLink

__all__ = ["FakeClock", "FakeLink", "Recorder"]


class Recorder:
    """Baseline :class:`~serialkit.DeviceHandler`: records every callback.

    Tests that need behaviour (a handshake, a crash, a flush) subclass and
    override; everything else asserts against these lists.
    """

    def __init__(self) -> None:
        self.frames: list[bytes] = []
        self.turns = 0
        self.connects = 0
        self.disconnects: list[Exception | None] = []
        # Frame count at the end of each turn, for asserting where turns fall.
        self.turn_boundaries: list[int] = []

    def on_frame(self, frame: bytes) -> None:
        self.frames.append(frame)

    def on_turn(self) -> None:
        self.turns += 1
        self.turn_boundaries.append(len(self.frames))

    async def on_connect(self) -> None:
        self.connects += 1

    def on_disconnect(self, exc: Exception | None) -> None:
        self.disconnects.append(exc)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def link() -> FakeLink:
    return FakeLink()


@pytest.fixture
def handler() -> Recorder:
    return Recorder()
