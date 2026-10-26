"""Exponential backoff with full jitter for jobqueue."""

from __future__ import annotations

import random
from typing import Protocol


class RngProtocol(Protocol):
    """Protocol for random number generators."""

    def uniform(self, a: float, b: float) -> float: ...


def compute_max_backoff(
    attempt: int,
    base: float = 1.0,
    max_backoff: float = 60.0,
) -> float:
    """Compute the maximum backoff limit before jitter: min(max_backoff, base * 2^(attempt - 1))."""
    if attempt < 1:
        attempt = 1
    factor = 2 ** (attempt - 1)
    raw = float(base * factor)
    return float(min(max_backoff, raw))


def compute_backoff(
    attempt: int,
    base: float = 1.0,
    max_backoff: float = 60.0,
    rng: RngProtocol | None = None,
) -> float:
    """Compute backoff with full jitter: uniform(0, min(max_backoff, base * 2^(attempt - 1)))."""
    cap = compute_max_backoff(attempt=attempt, base=base, max_backoff=max_backoff)
    if rng is None:
        rng = random.Random()
    return float(rng.uniform(0.0, cap))
