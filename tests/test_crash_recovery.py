"""Tests for worker crash recovery and lease expiration reclaiming."""

from __future__ import annotations

import multiprocessing as mp
import time
from pathlib import Path
from typing import Any

import pytest

from jobqueue.models import AttemptOutcome, JobStatus
from jobqueue.queue import JobQueue


def _crashing_worker_process(db_path_str: str, worker_id: str, started_event: Any) -> None:
    """Worker process that claims a job with a short lease and then hangs until killed."""
    q = JobQueue(db_path_str)
    job = q.claim(worker_id=worker_id, lease_duration=1.0)
    assert job is not None
    started_event.set()
    # Intentionally do not ack or close; simulate crash/hang
    time.sleep(60.0)


@pytest.mark.slow
def test_crash_recovery_expired_lease_reclaimed(tmp_path: Path) -> None:
    """When a worker process holding a lease crashes, the expired job is reclaimed.

    The second worker executes the job, marking the crashed attempt as lease_expired.
    """
    db_path = tmp_path / "crash_recovery.db"
    main_q = JobQueue(db_path)

    job = main_q.enqueue("important_task", {"order_id": 999}, max_attempts=3)
    main_q.close()

    ctx = mp.get_context("spawn")
    started_event = ctx.Event()

    # Launch crashing worker
    p = ctx.Process(
        target=_crashing_worker_process,
        args=(str(db_path), "worker-crashed", started_event),
    )
    p.start()

    # Wait until it claims the job
    assert started_event.wait(timeout=5.0)

    # Abruptly kill the worker process to simulate an unhandled crash / SIGKILL
    p.terminate()
    p.join(timeout=2.0)
    assert not p.is_alive()

    # Verify job is still running and held by the dead worker
    check_q = JobQueue(db_path)
    job_before = check_q.get(job.id)
    assert job_before is not None
    assert job_before.status == JobStatus.RUNNING
    assert job_before.lease_owner == "worker-crashed"
    assert job_before.lease_expires_at is not None

    # Wait for the lease to expire (lease_duration was 1.0s)
    remaining = job_before.lease_expires_at - time.time()
    if remaining > 0:
        time.sleep(remaining + 0.1)

    # Worker 2 now claims the expired job
    claimed2 = check_q.claim(worker_id="worker-healthy", lease_duration=10.0)
    assert claimed2 is not None
    assert claimed2.id == job.id
    assert claimed2.lease_owner == "worker-healthy"
    assert claimed2.attempts == 2

    # Worker 2 successfully acknowledges the job
    done = check_q.ack(claimed2.id, claimed2.lease_token or "", worker_id="worker-healthy")
    assert done.status == JobStatus.DONE

    # Verify attempt history
    attempts = check_q.get_attempts(job.id)
    assert len(attempts) == 2
    assert attempts[0].worker_id == "worker-crashed"
    assert attempts[0].outcome == AttemptOutcome.LEASE_EXPIRED
    assert attempts[1].worker_id == "worker-healthy"
    assert attempts[1].outcome == AttemptOutcome.SUCCESS

    check_q.close()
