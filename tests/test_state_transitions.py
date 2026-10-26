"""Tests for valid and illegal job lifecycle state transitions."""

from __future__ import annotations

import pytest

from jobqueue.clock import FakeClock
from jobqueue.models import (
    AttemptOutcome,
    InvalidStateTransitionError,
    JobStatus,
)
from jobqueue.queue import JobQueue


def test_valid_lifecycle_success(queue: JobQueue, fake_clock: FakeClock) -> None:
    """Test valid transition: queued -> running -> done."""
    job = queue.enqueue("task_a", {"val": 1})
    assert job.status == JobStatus.QUEUED

    claimed = queue.claim("worker-1", lease_duration=10.0)
    assert claimed is not None
    assert claimed.id == job.id
    assert claimed.status == JobStatus.RUNNING
    assert claimed.attempts == 1
    assert claimed.lease_token is not None

    done_job = queue.ack(claimed.id, claimed.lease_token, worker_id="worker-1")
    assert done_job.status == JobStatus.DONE
    assert done_job.lease_token is None

    attempts = queue.get_attempts(job.id)
    assert len(attempts) == 1
    assert attempts[0].outcome == AttemptOutcome.SUCCESS


def test_valid_lifecycle_retry_to_queued(queue: JobQueue, fake_clock: FakeClock) -> None:
    """Test valid transition: queued -> running -> queued (retry) -> running -> done."""
    job = queue.enqueue("task_a", {"val": 1}, max_attempts=3)
    claimed = queue.claim("worker-1", lease_duration=10.0)
    assert claimed is not None
    assert claimed.lease_token is not None

    nacked = queue.nack(
        claimed.id,
        claimed.lease_token,
        worker_id="worker-1",
        error="Temporary error",
    )
    assert nacked.status == JobStatus.QUEUED
    assert nacked.run_at >= fake_clock.now()
    assert nacked.attempts == 1
    assert nacked.last_error == "Temporary error"

    # Fast forward clock past backoff
    fake_clock.advance(100.0)

    # Next attempt
    claimed2 = queue.claim("worker-2", lease_duration=10.0)
    assert claimed2 is not None
    assert claimed2.id == job.id
    assert claimed2.attempts == 2
    assert claimed2.lease_token is not None

    done_job = queue.ack(claimed2.id, claimed2.lease_token, worker_id="worker-2")
    assert done_job.status == JobStatus.DONE

    attempts = queue.get_attempts(job.id)
    assert len(attempts) == 2
    assert attempts[0].outcome == AttemptOutcome.FAILED
    assert attempts[1].outcome == AttemptOutcome.SUCCESS


def test_valid_lifecycle_retry_exhaustion_to_dead(queue: JobQueue) -> None:
    """Test valid transition: queued -> running -> dead when max attempts reached."""
    job = queue.enqueue("task_a", {"val": 1}, max_attempts=1)
    claimed = queue.claim("worker-1", lease_duration=10.0)
    assert claimed is not None
    assert claimed.lease_token is not None

    dead_job = queue.nack(
        claimed.id,
        claimed.lease_token,
        worker_id="worker-1",
        error="Fatal error",
    )
    assert dead_job.status == JobStatus.DEAD
    assert dead_job.attempts == 1
    assert dead_job.last_error == "Fatal error"

    attempts = queue.get_attempts(job.id)
    assert len(attempts) == 1
    assert attempts[0].outcome == AttemptOutcome.FAILED


def test_valid_lifecycle_requeue_from_dead(queue: JobQueue) -> None:
    """Test valid transition: dead -> queued via requeue with attempts reset to 0."""
    job = queue.enqueue("task_a", {"val": 1}, max_attempts=1)
    claimed = queue.claim("worker-1", lease_duration=10.0)
    assert claimed is not None
    assert claimed.lease_token is not None

    queue.nack(
        claimed.id,
        claimed.lease_token,
        worker_id="worker-1",
        error="Fatal error",
    )

    requeued = queue.requeue(job.id)
    assert requeued.status == JobStatus.QUEUED
    assert requeued.attempts == 0
    assert requeued.last_error is None

    # History is kept
    attempts = queue.get_attempts(job.id)
    assert len(attempts) == 1
    assert attempts[0].outcome == AttemptOutcome.FAILED


def test_illegal_ack_on_queued_job(queue: JobQueue) -> None:
    """Cannot ack a queued job."""
    job = queue.enqueue("task_a", {"val": 1})
    with pytest.raises(InvalidStateTransitionError, match="Must be 'running'"):
        queue.ack(job.id, "fake-token", worker_id="w1")


def test_illegal_nack_on_queued_job(queue: JobQueue) -> None:
    """Cannot nack a queued job."""
    job = queue.enqueue("task_a", {"val": 1})
    with pytest.raises(InvalidStateTransitionError, match="Must be 'running'"):
        queue.nack(job.id, "fake-token", worker_id="w1", error="err")


def test_illegal_ack_on_done_job(queue: JobQueue) -> None:
    """Cannot ack an already done job."""
    job = queue.enqueue("task_a", {"val": 1})
    claimed = queue.claim("w1")
    assert claimed is not None
    token = claimed.lease_token
    assert token is not None

    queue.ack(job.id, token, worker_id="w1")

    with pytest.raises(InvalidStateTransitionError, match="Must be 'running'"):
        queue.ack(job.id, token, worker_id="w1")


def test_illegal_nack_on_done_job(queue: JobQueue) -> None:
    """Cannot nack an already done job."""
    job = queue.enqueue("task_a", {"val": 1})
    claimed = queue.claim("w1")
    assert claimed is not None
    token = claimed.lease_token
    assert token is not None

    queue.ack(job.id, token, worker_id="w1")

    with pytest.raises(InvalidStateTransitionError, match="Must be 'running'"):
        queue.nack(job.id, token, worker_id="w1", error="err")


def test_illegal_requeue_on_queued_job(queue: JobQueue) -> None:
    """Cannot requeue a queued job."""
    job = queue.enqueue("task_a", {"val": 1})
    with pytest.raises(InvalidStateTransitionError, match="Must be 'dead'"):
        queue.requeue(job.id)


def test_illegal_requeue_on_running_job(queue: JobQueue) -> None:
    """Cannot requeue a running job."""
    job = queue.enqueue("task_a", {"val": 1})
    claimed = queue.claim("w1")
    assert claimed is not None

    with pytest.raises(InvalidStateTransitionError, match="Must be 'dead'"):
        queue.requeue(job.id)


def test_illegal_requeue_on_done_job(queue: JobQueue) -> None:
    """Cannot requeue a done job."""
    job = queue.enqueue("task_a", {"val": 1})
    claimed = queue.claim("w1")
    assert claimed is not None
    assert claimed.lease_token is not None
    queue.ack(job.id, claimed.lease_token, worker_id="w1")

    with pytest.raises(InvalidStateTransitionError, match="Must be 'dead'"):
        queue.requeue(job.id)


def test_illegal_ack_on_dead_job(queue: JobQueue) -> None:
    """Cannot ack a dead job."""
    job = queue.enqueue("task_a", {"val": 1}, max_attempts=1)
    claimed = queue.claim("w1")
    assert claimed is not None
    assert claimed.lease_token is not None
    queue.nack(job.id, claimed.lease_token, worker_id="w1", error="fail")

    with pytest.raises(InvalidStateTransitionError, match="Must be 'running'"):
        queue.ack(job.id, claimed.lease_token, worker_id="w1")
