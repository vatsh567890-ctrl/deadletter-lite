"""Tests for dead-letter queue behavior and requeuing."""

from __future__ import annotations

from pathlib import Path

from jobqueue.clock import FakeClock
from jobqueue.models import AttemptOutcome, JobStatus
from jobqueue.queue import JobQueue


def test_dead_letter_exhaustion_and_requeue(tmp_path: Path) -> None:
    """A job that always fails reaches dead after max_attempts with the full error history.

    Requeue returns it to queued with attempts reset and history preserved.
    """
    clock = FakeClock(initial_time=5000.0)
    q = JobQueue(tmp_path / "dlq.db", clock=clock)

    try:
        max_attempts = 3
        job = q.enqueue("failing_task", {"input": "bad"}, max_attempts=max_attempts)
        assert job.status == JobStatus.QUEUED

        for attempt_idx in range(1, max_attempts + 1):
            claim = q.claim(f"worker-{attempt_idx}", lease_duration=10.0)
            assert claim is not None
            assert claim.attempts == attempt_idx
            token = claim.lease_token
            assert token is not None

            error_msg = f"Crash in attempt {attempt_idx}"
            nacked = q.nack(
                claim.id,
                token,
                worker_id=f"worker-{attempt_idx}",
                error=error_msg,
            )

            if attempt_idx < max_attempts:
                assert nacked.status == JobStatus.QUEUED
                assert nacked.attempts == attempt_idx
                # Advance clock past backoff
                clock.advance(100.0)
            else:
                assert nacked.status == JobStatus.DEAD
                assert nacked.attempts == max_attempts
                assert nacked.last_error == error_msg

        # Ensure dead job is not claimable
        clock.advance(1000.0)
        assert q.claim("worker-after-dead") is None

        # Verify full error history
        attempts = q.get_attempts(job.id)
        assert len(attempts) == max_attempts
        for idx, att in enumerate(attempts, start=1):
            assert att.attempt_number == idx
            assert att.worker_id == f"worker-{idx}"
            assert att.outcome == AttemptOutcome.FAILED
            assert att.error == f"Crash in attempt {idx}"

        # Requeue job from dead -> queued
        requeued = q.requeue(job.id)
        assert requeued.status == JobStatus.QUEUED
        assert requeued.attempts == 0
        assert requeued.last_error is None

        # Attempt history is still preserved!
        history_after_requeue = q.get_attempts(job.id)
        assert len(history_after_requeue) == max_attempts

        # Requeued job can now be claimed and executed
        new_claim = q.claim("worker-reclaim", lease_duration=10.0)
        assert new_claim is not None
        assert new_claim.id == job.id
        assert new_claim.attempts == 1
        assert new_claim.lease_token is not None

        done = q.ack(new_claim.id, new_claim.lease_token, worker_id="worker-reclaim")
        assert done.status == JobStatus.DONE

        # Total attempts now include attempt 1 of the new run
        final_history = q.get_attempts(job.id)
        assert len(final_history) == max_attempts + 1
        assert final_history[-1].outcome == AttemptOutcome.SUCCESS
        assert final_history[-1].worker_id == "worker-reclaim"
    finally:
        q.close()
