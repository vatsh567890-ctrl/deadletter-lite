"""Clock protocols and implementations for jobqueue."""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    """Protocol for abstracting time sources."""

    def now(self) -> float:
        """Return the current timestamp in epoch seconds."""
        ...


class SystemClock:
    """Production clock using system wall-clock time."""

    def now(self) -> float:
        """Return current time from the OS."""
        return time.time()


class FakeClock:
    """Deterministic, manually-advanced clock for testing."""

    def __init__(self, initial_time: float = 1700000000.0) -> None:
        self._current_time: float = initial_time

    def now(self) -> float:
        """Return current artificial time."""
        return self._current_time

    def advance(self, seconds: float) -> None:
        """Advance artificial time by a positive number of seconds."""
        if seconds < 0:
            msg = f"Cannot advance clock by negative time: {seconds}"
            raise ValueError(msg)
        self._current_time += seconds

    def set(self, timestamp: float) -> None:
        """Directly set the clock timestamp."""
        self._current_time = timestamp
