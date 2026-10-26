"""Tests for fencing tokens protecting against stale worker writes."""

from __future__ import annotations

import pytest

from jobqueue.clock import FakeClock
from jobqueue.models import (
    AttemptOutcome,
    JobStatus,
    LeaseLostError,
)
from jobqueue.queue import JobQueue


def test_fencing_stale_worker_ack_rejected(queue: JobQueue, fake_clock: FakeClock) -> None:
    """A stale worker whose lease was reclaimed cannot ack; the new worker completes the job."""
    job = queue.enqueue("task_a", {"step": 1})

    # Worker 1 claims with lease duration 5 seconds
    claim1 = queue.claim("worker-1", lease_duration=5.0)
    assert claim1 is not None
    token1 = claim1.lease_token
    assert token1 is not None
    assert claim1.attempts == 1

    # Worker 1 stalls; fake clock advances past lease expiry
    fake_clock.advance(6.0)

    # Worker 2 claims the expired job
    claim2 = queue.claim("worker-2", lease_duration=5.0)
    assert claim2 is not None
    token2 = claim2.lease_token
    assert token2 is not None
    assert token2 != token1
    assert claim2.attempts == 2

    # Worker 1 wakes up and attempts to ack with stale token1 -> rejected
    with pytest.raises(LeaseLostError):
        queue.ack(job.id, token1, worker_id="worker-1")

    # Worker 2 successfully acks
    done = queue.ack(job.id, token2, worker_id="worker-2")
    assert done.status == JobStatus.DONE

    # Verify attempt history shows worker-1 expired and worker-2 succeeded
    attempts = queue.get_attempts(job.id)
    assert len(attempts) == 2
    assert attempts[0].worker_id == "worker-1"
    assert attempts[0].outcome == AttemptOutcome.LEASE_EXPIRED
    assert attempts[1].worker_id == "worker-2"
    assert attempts[1].outcome == AttemptOutcome.SUCCESS


def test_fencing_stale_worker_renewal_rejected(queue: JobQueue, fake_clock: FakeClock) -> None:
    """A stale worker cannot renew its lease after another worker has claimed it."""
    job = queue.enqueue("task_a", {"step": 1})
    claim1 = queue.claim("worker-1", lease_duration=5.0)
    assert claim1 is not None
    token1 = claim1.lease_token
    assert token1 is not None

    fake_clock.advance(6.0)

    # Worker 2 claims
    claim2 = queue.claim("worker-2", lease_duration=5.0)
    assert claim2 is not None

    # Worker 1 attempts renewal -> rejected
    with pytest.raises(LeaseLostError):
        queue.renew(job.id, token1, lease_duration=5.0)


def test_fencing_stale_worker_nack_rejected(queue: JobQueue, fake_clock: FakeClock) -> None:
    """A stale worker cannot nack or modify a job after its lease is reclaimed."""
    job = queue.enqueue("task_a", {"step": 1})
    claim1 = queue.claim("worker-1", lease_duration=5.0)
    assert claim1 is not None
    token1 = claim1.lease_token
    assert token1 is not None

    fake_clock.advance(6.0)

    # Worker 2 claims
    claim2 = queue.claim("worker-2", lease_duration=5.0)
    assert claim2 is not None

    # Worker 1 tries to nack -> rejected
    with pytest.raises(LeaseLostError):
        queue.nack(job.id, token1, worker_id="worker-1", error="Late failure")


def test_fencing_stale_worker_release_rejected(queue: JobQueue, fake_clock: FakeClock) -> None:
    """A stale worker cannot release a job after its lease has expired and been reclaimed."""
    job = queue.enqueue("task_a", {"step": 1})
    claim1 = queue.claim("worker-1", lease_duration=5.0)
    assert claim1 is not None
    token1 = claim1.lease_token
    assert token1 is not None

    fake_clock.advance(6.0)

    claim2 = queue.claim("worker-2", lease_duration=5.0)
    assert claim2 is not None

    with pytest.raises(LeaseLostError):
        queue.release(job.id, token1, worker_id="worker-1")
