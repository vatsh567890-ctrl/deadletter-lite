"""Tests for jobqueue command-line interface."""

from __future__ import annotations

import io
import os
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from jobqueue.cli import main
from jobqueue.models import JobStatus
from jobqueue.queue import JobQueue


def test_cli_enqueue_and_inspect(tmp_path: Path) -> None:
    """CLI enqueue creates a job and inspect lists it."""
    db_path = str(tmp_path / "cli_test.db")

    stdout_buf = io.StringIO()
    with redirect_stdout(stdout_buf):
        exit_code = main(
            [
                "enqueue",
                "--task",
                "send_email",
                "--payload",
                '{"to": "test@example.com"}',
                "--db",
                db_path,
            ]
        )
    assert exit_code == 0
    output = stdout_buf.getvalue()
    assert "Enqueued job" in output
    assert "send_email" in output

    # Inspect
    inspect_buf = io.StringIO()
    with redirect_stdout(inspect_buf):
        exit_code = main(["inspect", "--db", db_path])
    assert exit_code == 0
    inspect_output = inspect_buf.getvalue()
    assert "send_email" in inspect_output
    assert "queued" in inspect_output


def test_cli_inspect_single_job(tmp_path: Path) -> None:
    """CLI inspect --job-id displays job metadata and attempts."""
    db_path = str(tmp_path / "cli_inspect.db")
    q = JobQueue(db_path)
    job = q.enqueue("calc", '{"val": 42}')
    q.close()

    stdout_buf = io.StringIO()
    with redirect_stdout(stdout_buf):
        exit_code = main(["inspect", "--db", db_path, "--job-id", job.id])
    assert exit_code == 0
    output = stdout_buf.getvalue()
    assert job.id in output
    assert "calc" in output
    assert '{"val": 42}' in output


def test_cli_requeue_dead_job(tmp_path: Path) -> None:
    """CLI requeue resets dead job to queued status."""
    db_path = str(tmp_path / "cli_requeue.db")
    q = JobQueue(db_path)
    job = q.enqueue("fail", "{}", max_attempts=1)
    claimed = q.claim("w1")
    assert claimed is not None
    q.nack(claimed.id, claimed.lease_token or "", worker_id="w1", error="fatal")
    q.close()

    # Requeue via CLI
    stdout_buf = io.StringIO()
    with redirect_stdout(stdout_buf):
        exit_code = main(["requeue", job.id, "--db", db_path])
    assert exit_code == 0
    assert "Requeued dead job" in stdout_buf.getvalue()

    # Verify status in DB
    verify_q = JobQueue(db_path)
    updated = verify_q.get(job.id)
    assert updated is not None
    assert updated.status == JobStatus.QUEUED
    assert updated.attempts == 0
    verify_q.close()


def test_cli_requeue_nonexistent_or_not_dead(tmp_path: Path) -> None:
    """CLI requeue returns error code 1 if job not found or not dead."""
    db_path = str(tmp_path / "cli_err.db")
    q = JobQueue(db_path)
    job = q.enqueue("running_task", "{}")
    q.close()

    stderr_buf = io.StringIO()
    with redirect_stderr(stderr_buf):
        exit_code = main(["requeue", "nonexistent-id", "--db", db_path])
    assert exit_code == 1

    with redirect_stderr(stderr_buf):
        exit_code = main(["requeue", job.id, "--db", db_path])
    assert exit_code == 1


def test_cli_environment_variable_defaults(tmp_path: Path) -> None:
    """CLI uses JOBQUEUE_DB environment variable when --db is omitted."""
    db_path = str(tmp_path / "env_test.db")
    os.environ["JOBQUEUE_DB"] = db_path

    stdout_buf = io.StringIO()
    with redirect_stdout(stdout_buf):
        exit_code = main(
            [
                "enqueue",
                "--task",
                "env_task",
                "--payload",
                '{"from": "env"}',
            ]
        )
    assert exit_code == 0

    assert Path(db_path).exists()
    q = JobQueue(db_path)
    jobs = q.list_jobs()
    assert len(jobs) == 1
    assert jobs[0].task_name == "env_task"
    q.close()
