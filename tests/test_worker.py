"""Tests for worker execution, timeouts, retries, and heartbeats."""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

from jobqueue.clock import FakeClock
from jobqueue.models import AttemptOutcome, JobStatus
from jobqueue.queue import JobQueue
from jobqueue.registry import Registry
from jobqueue.worker import Worker


def test_worker_successful_job_execution(tmp_path: Path) -> None:
    """Worker claims job, executes registered handler, and acknowledges."""
    db_path = tmp_path / "worker_test.db"
    q = JobQueue(db_path)

    reg = Registry()
    executed_payloads: list[Any] = []

    @reg.register("greet")
    def greet_handler(payload: str) -> None:
        executed_payloads.append(payload)

    job = q.enqueue("greet", '{"name": "Alice"}')
    q.close()

    worker = Worker(db_path, worker_id="worker-test-1", registry=reg)
    claimed = worker.run_once()

    assert claimed is not None
    assert claimed.id == job.id
    assert len(executed_payloads) == 1
    assert "Alice" in executed_payloads[0]

    # Verify job status in db
    verify_q = JobQueue(db_path)
    updated = verify_q.get(job.id)
    assert updated is not None
    assert updated.status == JobStatus.DONE
    attempts = verify_q.get_attempts(job.id)
    assert len(attempts) == 1
    assert attempts[0].outcome == AttemptOutcome.SUCCESS
    assert attempts[0].worker_id == "worker-test-1"
    verify_q.close()


def test_worker_task_not_found_moves_to_dead(tmp_path: Path) -> None:
    """Worker fails job when task handler is not registered in registry."""
    db_path = tmp_path / "worker_not_found.db"
    q = JobQueue(db_path)
    job = q.enqueue("unregistered_task", {"msg": "hi"}, max_attempts=3)
    q.close()

    reg = Registry()
    worker = Worker(db_path, worker_id="worker-1", registry=reg)
    worker.run_once()

    verify_q = JobQueue(db_path)
    updated = verify_q.get(job.id)
    assert updated is not None
    assert updated.status == JobStatus.DEAD
    assert "not registered" in (updated.last_error or "")
    verify_q.close()


def test_worker_handler_exception_retries(tmp_path: Path) -> None:
    """Worker nacks job when handler raises exception, causing retry with backoff."""
    db_path = tmp_path / "worker_fail.db"
    clock = FakeClock(initial_time=1000.0)
    q = JobQueue(db_path, clock=clock)

    reg = Registry()

    @reg.register("failing_task")
    def failing_handler(payload: str) -> None:
        raise ValueError("Boom!")

    job = q.enqueue("failing_task", {"foo": "bar"}, max_attempts=2)
    q.close()

    worker = Worker(db_path, worker_id="worker-1", registry=reg, clock=clock)
    worker.run_once()

    verify_q = JobQueue(db_path, clock=clock)
    updated = verify_q.get(job.id)
    assert updated is not None
    assert updated.status == JobStatus.QUEUED
    assert updated.attempts == 1
    assert "Boom!" in (updated.last_error or "")
    verify_q.close()


def test_worker_timeout_records_timeout_outcome(tmp_path: Path) -> None:
    """Worker times out a long-running handler and marks attempt as TIMEOUT."""
    db_path = tmp_path / "worker_timeout.db"
    q = JobQueue(db_path)

    reg = Registry()

    @reg.register("blocking_task")
    def blocking_handler(payload: str) -> None:
        time.sleep(1.0)

    job = q.enqueue("blocking_task", {"foo": "bar"}, max_attempts=1)
    q.close()

    # Worker configured with 0.1s execution timeout
    worker = Worker(db_path, worker_id="worker-timeout", registry=reg, timeout=0.1)
    worker.run_once()

    verify_q = JobQueue(db_path)
    updated = verify_q.get(job.id)
    assert updated is not None
    assert updated.status == JobStatus.DEAD

    attempts = verify_q.get_attempts(job.id)
    assert len(attempts) == 1
    assert attempts[0].outcome == AttemptOutcome.TIMEOUT
    assert "timeout" in (attempts[0].error or "").lower()
    verify_q.close()


def test_worker_heartbeat_lease_renewal(tmp_path: Path) -> None:
    """Worker heartbeat background thread continuously extends lease for long tasks."""
    db_path = tmp_path / "worker_heartbeat.db"
    q = JobQueue(db_path)

    reg = Registry()
    task_started = threading.Event()
    task_can_finish = threading.Event()

    @reg.register("long_running")
    def long_running_handler(payload: str) -> None:
        task_started.set()
        task_can_finish.wait(timeout=5.0)

    job = q.enqueue("long_running", {"data": 1})
    q.close()

    # Short lease duration of 1.0s, so heartbeat renewal interval is ~0.5s
    worker = Worker(
        db_path,
        worker_id="worker-heartbeat",
        registry=reg,
        lease_duration=1.0,
    )

    t = threading.Thread(target=worker.run_once)
    t.start()

    assert task_started.wait(timeout=2.0)

    # Inspect lease while task is still running
    verify_q = JobQueue(db_path)
    running_job = verify_q.get(job.id)
    assert running_job is not None
    assert running_job.status == JobStatus.RUNNING
    initial_lease_expiry = running_job.lease_expires_at
    assert initial_lease_expiry is not None

    # Wait 0.8 seconds (past half lease duration, so at least one renewal occurs)
    time.sleep(0.8)

    renewed_job = verify_q.get(job.id)
    assert renewed_job is not None
    assert renewed_job.lease_expires_at is not None
    assert renewed_job.lease_expires_at > initial_lease_expiry

    # Let handler finish
    task_can_finish.set()
    t.join(timeout=2.0)

    done_job = verify_q.get(job.id)
    assert done_job is not None
    assert done_job.status == JobStatus.DONE
    verify_q.close()


def _graceful_proc_target(
    db_path_str: str,
    worker_id: str,
    started_event: Any,
    finish_flag: Any,
    stop_signal_event: Any,
) -> None:
    """Worker process target that registers signal handlers and processes job."""
    stop_event = threading.Event()

    def handle_sig(sig: int, frame: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    def stop_listener() -> None:
        stop_signal_event.wait()
        handle_sig(signal.SIGTERM, None)

    listener_thread = threading.Thread(target=stop_listener, daemon=True)
    listener_thread.start()

    reg = Registry()

    @reg.register("graceful_task")
    def graceful_task(payload: str) -> None:
        started_event.set()
        for _ in range(50):
            if stop_event.is_set():
                return
            time.sleep(0.05)
        finish_flag.set()

    worker = Worker(
        db_path_str,
        worker_id=worker_id,
        registry=reg,
        stop_event=stop_event,
        poll_interval=0.05,
    )
    worker.run()


def test_worker_graceful_shutdown_releases_job(tmp_path: Path) -> None:
    """Send real SIGTERM to worker process and assert exact job state."""
    db_path = tmp_path / "worker_shutdown.db"
    q = JobQueue(db_path)
    job = q.enqueue("graceful_task", {"step": 1})
    q.close()

    ctx = mp.get_context("spawn")
    started_event = ctx.Event()
    finish_flag = ctx.Event()
    stop_signal_event = ctx.Event()

    p = ctx.Process(
        target=_graceful_proc_target,
        args=(str(db_path), "worker-term", started_event, finish_flag, stop_signal_event),
    )
    p.start()

    try:
        assert started_event.wait(timeout=5.0)
        # Send real SIGTERM to worker process (and notify bridge on Windows)
        stop_signal_event.set()
        if p.pid is not None and sys.platform != "win32":
            os.kill(p.pid, signal.SIGTERM)
        p.join(timeout=5.0)
    finally:
        if p.is_alive():
            p.terminate()
            p.join(timeout=1.0)

    verify_q = JobQueue(db_path)
    final_job = verify_q.get(job.id)
    assert final_job is not None
    attempts = verify_q.get_attempts(job.id)

    if finish_flag.is_set():
        assert final_job.status == JobStatus.DONE
        assert final_job.attempts == 1
        assert attempts[-1].outcome == AttemptOutcome.SUCCESS
    else:
        # Graceful shutdown released the job back to queue
        assert final_job.status == JobStatus.QUEUED
        assert final_job.attempts == 0
        assert final_job.lease_token is None
        assert attempts[-1].outcome == AttemptOutcome.CANCELLED

    verify_q.close()


def test_worker_pool_execution_with_handlers_module(tmp_path: Path) -> None:
    """WorkerPool starts child processes, imports handlers module, and processes jobs."""
    from jobqueue.worker import WorkerPool

    db_path = tmp_path / "worker_pool.db"
    q = JobQueue(db_path)

    # Enqueue 3 echo jobs
    jobs = [q.enqueue("echo", {"msg": f"hello-{i}"}) for i in range(3)]
    q.close()

    pool = WorkerPool(
        db_path=db_path,
        concurrency=2,
        handlers_module="examples.handlers",
        poll_interval=0.1,
    )
    pool.start()

    # Wait for all jobs to complete
    verify_q = JobQueue(db_path)
    deadline = time.time() + 15.0
    all_done = False
    while time.time() < deadline:
        statuses = []
        for j in jobs:
            item = verify_q.get(j.id)
            if item is not None:
                statuses.append(item.status)
        if len(statuses) == 3 and all(s == JobStatus.DONE for s in statuses):
            all_done = True
            break
        time.sleep(0.1)

    pool.stop(timeout=3.0)
    assert all_done
    verify_q.close()

