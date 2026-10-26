"""Atomic claim stress test exercising high concurrency across multiple processes."""

from __future__ import annotations

import multiprocessing as mp
import time
from pathlib import Path
from typing import Any

import pytest

from jobqueue.models import JobStatus
from jobqueue.queue import JobQueue
from jobqueue.storage import immediate_transaction


def _stress_worker_entry(
    db_path_str: str,
    worker_id: str,
    results_queue: Any,
) -> None:
    """Worker process loop that claims and acks jobs until queue is empty."""
    q = JobQueue(db_path_str, timeout=30.0)
    claims: list[tuple[str, str]] = []
    consecutive_empty = 0

    try:
        while True:
            job = q.claim(worker_id=worker_id, lease_duration=120.0)
            if job is None:
                consecutive_empty += 1
                if consecutive_empty >= 25:
                    break
                time.sleep(0.01)
                continue

            consecutive_empty = 0
            token = str(job.lease_token)
            claims.append((job.id, token))
            q.ack(job.id, token, worker_id=worker_id)
    finally:
        q.close()
        results_queue.put(claims)


@pytest.mark.slow
def test_atomic_claim_stress_8_workers_2000_jobs(tmp_path: Path) -> None:
    """Stress test: 8 worker processes, 2,000 jobs, handlers that succeed.

    Assert every job completes exactly once and no lease_token is shared by two claims.
    """
    db_path = tmp_path / "stress.db"
    main_q = JobQueue(db_path, timeout=30.0)

    num_jobs = 2000
    num_workers = 8

    # Bulk enqueue 2,000 jobs efficiently within a single transaction
    now = time.time()
    with immediate_transaction(main_q.storage.conn):
        for i in range(num_jobs):
            job_id = f"job-{i:04d}"
            main_q.storage.conn.execute(
                """
                INSERT INTO jobs (
                    id, queue_name, task_name, payload, status, attempts,
                    max_attempts, run_at, lease_owner, lease_token,
                    lease_expires_at, created_at, updated_at, last_error
                ) VALUES (
                    ?, 'default', 'test_task', '{"index": 1}', 'queued', 0,
                    3, ?, NULL, NULL, NULL, ?, ?, NULL
                )
                """,
                (job_id, now, now, now),
            )
    main_q.close()

    # Launch 8 worker processes using spawn context
    ctx = mp.get_context("spawn")
    results_q = ctx.Queue()
    processes: list[Any] = []

    for w_idx in range(num_workers):
        worker_id = f"worker-{w_idx}"
        p = ctx.Process(
            target=_stress_worker_entry,
            args=(str(db_path), worker_id, results_q),
        )
        p.start()
        processes.append(p)

    all_claims: list[tuple[str, str]] = []
    for _ in range(num_workers):
        worker_claims = results_q.get(timeout=60.0)
        all_claims.extend(worker_claims)

    for p in processes:
        p.join(timeout=10.0)
        assert not p.is_alive()
        assert p.exitcode == 0

    # 1. Total jobs processed across all workers must equal exactly num_jobs
    assert len(all_claims) == num_jobs

    job_ids = [cid for cid, _ in all_claims]
    tokens = [tok for _, tok in all_claims]

    # 2. Every job was claimed exactly once (no duplicates)
    assert len(set(job_ids)) == num_jobs

    # 3. No lease_token is shared by two claims
    assert len(set(tokens)) == num_jobs

    # 4. Durable state verification in database
    verify_q = JobQueue(db_path, timeout=30.0)
    try:
        counts = verify_q.count_by_status()
        assert counts[JobStatus.DONE.value] == num_jobs
        assert counts[JobStatus.QUEUED.value] == 0
        assert counts[JobStatus.RUNNING.value] == 0
        assert counts[JobStatus.DEAD.value] == 0

        # Verify every job has attempts = 1
        cursor = verify_q.storage.conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE attempts = 1 AND status = 'done'"
        )
        assert cursor.fetchone()[0] == num_jobs

        # Verify attempts table has exactly 2,000 success records
        cursor = verify_q.storage.conn.execute(
            "SELECT COUNT(*) FROM attempts WHERE outcome = 'success'"
        )
        assert cursor.fetchone()[0] == num_jobs
    finally:
        verify_q.close()
