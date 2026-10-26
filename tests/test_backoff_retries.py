"""Tests for retries, exponential backoff, and full jitter using FakeClock and seeded RNG."""

from __future__ import annotations

import random
from pathlib import Path

from deadletter_lite.backoff import compute_max_backoff
from deadletter_lite.clock import FakeClock
from deadletter_lite.models import AttemptOutcome, JobStatus
from deadletter_lite.queue import JobQueue


def test_compute_backoff_formula_limits() -> None:
    """Verify compute_max_backoff respects min(max_backoff, base * 2^(attempt - 1))."""
    # base = 1.0, max = 60.0
    assert compute_max_backoff(attempt=1, base=1.0, max_backoff=60.0) == 1.0
    assert compute_max_backoff(attempt=2, base=1.0, max_backoff=60.0) == 2.0
    assert compute_max_backoff(attempt=3, base=1.0, max_backoff=60.0) == 4.0
    assert compute_max_backoff(attempt=4, base=1.0, max_backoff=60.0) == 8.0
    assert compute_max_backoff(attempt=5, base=1.0, max_backoff=60.0) == 16.0
    assert compute_max_backoff(attempt=6, base=1.0, max_backoff=60.0) == 32.0
    assert compute_max_backoff(attempt=7, base=1.0, max_backoff=60.0) == 60.0
    assert compute_max_backoff(attempt=10, base=1.0, max_backoff=60.0) == 60.0


def test_retries_with_exact_backoff_and_fake_clock(tmp_path: Path) -> None:
    """Test retries: handler failing twice then succeeding completes on attempt 3.

    Assert the exact backoff values produced by the formula with a seeded RNG.
    No time.sleep is used; FakeClock is advanced manually.
    """
    seed = 42
    test_clock = FakeClock(initial_time=1000.0)
    queue_rng = random.Random(seed)
    expected_rng = random.Random(seed)

    base = 2.0
    max_backoff = 30.0

    q = JobQueue(
        tmp_path / "retry.db",
        clock=test_clock,
        rng=queue_rng,
        backoff_base=base,
        max_backoff=max_backoff,
    )

    try:
        # Enqueue job with max_attempts=3
        job = q.enqueue("sample_task", {"foo": "bar"}, max_attempts=3)
        assert job.attempts == 0
        assert job.status == JobStatus.QUEUED

        # --- ATTEMPT 1 ---
        claim1 = q.claim("worker-1", lease_duration=10.0)
        assert claim1 is not None
        assert claim1.attempts == 1
        assert claim1.lease_token is not None

        # Calculate exact expected delay for attempt 1
        expected_cap_1 = min(max_backoff, base * (2 ** (1 - 1)))
        expected_delay_1 = expected_rng.uniform(0.0, expected_cap_1)

        # Worker 1 fails
        nack1 = q.nack(
            claim1.id,
            claim1.lease_token,
            worker_id="worker-1",
            error="Network flake 1",
        )
        assert nack1.status == JobStatus.QUEUED
        assert nack1.attempts == 1
        assert nack1.run_at == 1000.0 + expected_delay_1

        # While clock < run_at, claim should return None
        test_clock.set(1000.0 + expected_delay_1 - 0.001)
        assert q.claim("worker-1") is None

        # Advance clock to exact run_at
        test_clock.set(1000.0 + expected_delay_1)

        # --- ATTEMPT 2 ---
        claim2 = q.claim("worker-2", lease_duration=10.0)
        assert claim2 is not None
        assert claim2.attempts == 2
        assert claim2.lease_token is not None

        # Calculate exact expected delay for attempt 2
        expected_cap_2 = min(max_backoff, base * (2 ** (2 - 1)))
        expected_delay_2 = expected_rng.uniform(0.0, expected_cap_2)

        nack2 = q.nack(
            claim2.id,
            claim2.lease_token,
            worker_id="worker-2",
            error="Network flake 2",
        )
        assert nack2.status == JobStatus.QUEUED
        assert nack2.attempts == 2
        time_at_nack2 = test_clock.now()
        assert nack2.run_at == time_at_nack2 + expected_delay_2

        # Advance clock to exact run_at of attempt 3
        test_clock.set(time_at_nack2 + expected_delay_2)

        # --- ATTEMPT 3 ---
        claim3 = q.claim("worker-3", lease_duration=10.0)
        assert claim3 is not None
        assert claim3.attempts == 3
        assert claim3.lease_token is not None

        # Attempt 3 succeeds!
        done = q.ack(claim3.id, claim3.lease_token, worker_id="worker-3")
        assert done.status == JobStatus.DONE
        assert done.attempts == 3

        # Verify attempt history
        attempts = q.get_attempts(job.id)
        assert len(attempts) == 3
        assert attempts[0].attempt_number == 1
        assert attempts[0].outcome == AttemptOutcome.FAILED
        assert attempts[0].error == "Network flake 1"

        assert attempts[1].attempt_number == 2
        assert attempts[1].outcome == AttemptOutcome.FAILED
        assert attempts[1].error == "Network flake 2"

        assert attempts[2].attempt_number == 3
        assert attempts[2].outcome == AttemptOutcome.SUCCESS
    finally:
        q.close()
