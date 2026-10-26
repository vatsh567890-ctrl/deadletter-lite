"""Shared pytest fixtures and test hygiene configurations."""

from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path

import pytest

from jobqueue.clock import FakeClock
from jobqueue.queue import JobQueue

ENV_VARS_TO_CLEAR = [
    "JOBQUEUE_DB",
    "JOBQUEUE_HOST",
    "JOBQUEUE_PORT",
    "JOBQUEUE_CONCURRENCY",
    "JOBQUEUE_HANDLERS",
]


@pytest.fixture(autouse=True)
def clean_environment() -> Generator[None, None, None]:
    """Ensure tests run in an isolated environment with no leaking env vars."""
    original_values = {var: os.environ.get(var) for var in ENV_VARS_TO_CLEAR}
    for var in ENV_VARS_TO_CLEAR:
        os.environ.pop(var, None)

    yield

    for var, orig in original_values.items():
        if orig is not None:
            os.environ[var] = orig
        else:
            os.environ.pop(var, None)


@pytest.fixture
def fake_clock() -> FakeClock:
    """Fixture providing a deterministic fake clock."""
    return FakeClock(initial_time=1700000000.0)


@pytest.fixture
def temp_db(tmp_path: Path) -> Path:
    """Fixture providing a clean SQLite database path."""
    return tmp_path / "test_jobqueue.db"


@pytest.fixture
def queue(temp_db: Path, fake_clock: FakeClock) -> Generator[JobQueue, None, None]:
    """Fixture providing an initialized JobQueue instance with a fake clock."""
    q = JobQueue(temp_db, clock=fake_clock)
    yield q
    q.close()
