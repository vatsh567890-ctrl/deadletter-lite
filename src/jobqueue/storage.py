"""Durable storage layer and SQLite interactions for jobqueue."""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from jobqueue.models import (
    Attempt,
    AttemptOutcome,
    Job,
    JobNotFoundError,
    JobStatus,
    LeaseLostError,
    PayloadTooLargeError,
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    queue_name TEXT NOT NULL DEFAULT 'default',
    task_name TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('queued', 'running', 'done', 'dead')),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    run_at REAL NOT NULL,
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_claimable
ON jobs (status, run_at)
WHERE status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS idx_jobs_queue_status
ON jobs (queue_name, status);

CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL,
    outcome TEXT NOT NULL CHECK(
        outcome IN ('running', 'success', 'failed', 'timeout', 'lease_expired', 'cancelled')
    ),
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_attempts_job_id
ON attempts (job_id, attempt_number);
"""


def create_connection(
    db_path: str | Path,
    *,
    timeout: float = 10.0,
    read_only: bool = False,
) -> sqlite3.Connection:
    """Create and configure a new SQLite connection for the current process."""
    path_obj = Path(db_path)
    if not read_only:
        path_obj.parent.mkdir(parents=True, exist_ok=True)

    if read_only:
        uri = f"file:{path_obj.resolve().as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=timeout, isolation_level=None)
    else:
        conn = sqlite3.connect(str(path_obj), timeout=timeout, isolation_level=None)

    conn.row_factory = sqlite3.Row
    timeout_ms = int(timeout * 1000)
    # Configure WAL mode and busy timeout
    if not read_only:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute(f"PRAGMA busy_timeout = {timeout_ms};")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA foreign_keys = ON;")
    else:
        conn.execute(f"PRAGMA busy_timeout = {timeout_ms};")
        conn.execute("PRAGMA foreign_keys = ON;")

    return conn


@contextmanager
def immediate_transaction(conn: sqlite3.Connection) -> Generator[None, None, None]:
    """Execute block inside BEGIN IMMEDIATE to prevent SQLite deadlocks."""
    conn.execute("BEGIN IMMEDIATE;")
    try:
        yield
        if conn.in_transaction:
            conn.execute("COMMIT;")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK;")
        raise


def init_db(conn: sqlite3.Connection) -> None:
    """Initialize database schema and indices."""
    conn.executescript(SCHEMA_SQL)


class Storage:
    """Durable storage manager wrapping an isolated SQLite connection."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    @classmethod
    def open(
        cls,
        db_path: str | Path,
        *,
        timeout: float = 5.0,
        read_only: bool = False,
    ) -> Storage:
        """Open a new Storage instance with a fresh connection."""
        conn = create_connection(db_path, timeout=timeout, read_only=read_only)
        if not read_only:
            init_db(conn)
        return cls(conn)

    def close(self) -> None:
        """Close the underlying connection."""
        self.conn.close()

    def insert_job(
        self,
        *,
        job_id: str,
        queue_name: str,
        task_name: str,
        payload: str,
        max_attempts: int,
        run_at: float,
        created_at: float,
    ) -> Job:
        """Insert a newly enqueued job."""
        from jobqueue.models import MAX_PAYLOAD_SIZE

        if len(payload.encode("utf-8")) > MAX_PAYLOAD_SIZE:
            msg = f"Payload size exceeds limit of {MAX_PAYLOAD_SIZE} bytes"
            raise PayloadTooLargeError(msg)

        query = """
        INSERT INTO jobs (
            id, queue_name, task_name, payload, status, attempts,
            max_attempts, run_at, lease_owner, lease_token,
            lease_expires_at, created_at, updated_at, last_error
        ) VALUES (
            ?, ?, ?, ?, 'queued', 0,
            ?, ?, NULL, NULL,
            NULL, ?, ?, NULL
        )
        RETURNING *;
        """
        with immediate_transaction(self.conn):
            cursor = self.conn.execute(
                query,
                (
                    job_id,
                    queue_name,
                    task_name,
                    payload,
                    max_attempts,
                    run_at,
                    created_at,
                    created_at,
                ),
            )
            row = cursor.fetchone()
            return Job.from_row(row)

    def claim_job(
        self,
        *,
        worker_id: str,
        lease_token: str,
        lease_duration: float,
        now: float,
        queue_name: str | None = None,
    ) -> Job | None:
        """Atomically claim the next eligible job using UPDATE ... RETURNING."""
        lease_expires_at = now + lease_duration

        where_queue = "AND queue_name = ?" if queue_name else ""
        params: list[Any] = [
            worker_id,
            lease_token,
            lease_expires_at,
            now,
            now,
            now,
        ]
        if queue_name:
            params.append(queue_name)

        query = f"""
        UPDATE jobs
        SET status = 'running',
            lease_owner = ?,
            lease_token = ?,
            lease_expires_at = ?,
            attempts = attempts + 1,
            updated_at = ?
        WHERE id = (
            SELECT id FROM jobs
            WHERE ((status = 'queued' AND run_at <= ?)
               OR (status = 'running' AND lease_expires_at <= ?))
               {where_queue}
            ORDER BY run_at ASC, id ASC
            LIMIT 1
        )
        RETURNING *;
        """
        with immediate_transaction(self.conn):
            cursor = self.conn.execute(query, params)
            row = cursor.fetchone()
            if row is None:
                return None

            job = Job.from_row(row)

            # If there was a previous attempt that remained unfinished, mark it lease_expired
            self.conn.execute(
                """
                UPDATE attempts
                SET outcome = 'lease_expired',
                    finished_at = ?,
                    error = 'Lease expired while running'
                WHERE job_id = ? AND outcome = 'running'
                """,
                (now, job.id),
            )

            # Record the new attempt as running
            self.conn.execute(
                """
                INSERT INTO attempts (
                    job_id, attempt_number, worker_id, started_at,
                    finished_at, outcome, error
                ) VALUES (?, ?, ?, ?, NULL, 'running', NULL)
                """,
                (job.id, job.attempts, worker_id, now),
            )
            return job

    def ack_job(
        self,
        *,
        job_id: str,
        lease_token: str,
        worker_id: str,
        now: float,
    ) -> Job:
        """Acknowledge job completion under fencing protection."""
        query = """
        UPDATE jobs
        SET status = 'done',
            lease_owner = NULL,
            lease_token = NULL,
            lease_expires_at = NULL,
            updated_at = ?
        WHERE id = ? AND lease_token = ? AND status = 'running'
        RETURNING *;
        """
        with immediate_transaction(self.conn):
            cursor = self.conn.execute(query, (now, job_id, lease_token))
            row = cursor.fetchone()
            if row is None:
                msg = f"Lease lost for job {job_id} or invalid token {lease_token}"
                raise LeaseLostError(msg)

            job = Job.from_row(row)
            self.conn.execute(
                """
                UPDATE attempts
                SET outcome = 'success',
                    finished_at = ?
                WHERE id = (
                    SELECT id FROM attempts
                    WHERE job_id = ? AND outcome = 'running' AND worker_id = ?
                    ORDER BY id DESC LIMIT 1
                )
                """,
                (now, job.id, worker_id),
            )
            return job

    def nack_job(
        self,
        *,
        job_id: str,
        lease_token: str,
        worker_id: str,
        error: str,
        outcome: AttemptOutcome,
        retry_run_at: float | None,
        now: float,
    ) -> Job:
        """Nack a job with retry or dead-letter under fencing protection."""
        new_status = JobStatus.DEAD if retry_run_at is None else JobStatus.QUEUED
        next_run_at = now if retry_run_at is None else retry_run_at

        query = """
        UPDATE jobs
        SET status = ?,
            run_at = ?,
            lease_owner = NULL,
            lease_token = NULL,
            lease_expires_at = NULL,
            last_error = ?,
            updated_at = ?
        WHERE id = ? AND lease_token = ? AND status = 'running'
        RETURNING *;
        """
        with immediate_transaction(self.conn):
            cursor = self.conn.execute(
                query,
                (new_status.value, next_run_at, error, now, job_id, lease_token),
            )
            row = cursor.fetchone()
            if row is None:
                msg = f"Lease lost for job {job_id} or invalid token {lease_token}"
                raise LeaseLostError(msg)

            job = Job.from_row(row)
            self.conn.execute(
                """
                UPDATE attempts
                SET outcome = ?,
                    finished_at = ?,
                    error = ?
                WHERE id = (
                    SELECT id FROM attempts
                    WHERE job_id = ? AND outcome = 'running' AND worker_id = ?
                    ORDER BY id DESC LIMIT 1
                )
                """,
                (outcome.value, now, error, job.id, worker_id),
            )
            return job

    def renew_lease(
        self,
        *,
        job_id: str,
        lease_token: str,
        lease_duration: float,
        now: float,
    ) -> float:
        """Renew lease for an active running job under fencing protection."""
        new_lease_expires_at = now + lease_duration
        query = """
        UPDATE jobs
        SET lease_expires_at = ?,
            updated_at = ?
        WHERE id = ? AND lease_token = ? AND status = 'running'
        RETURNING lease_expires_at;
        """
        with immediate_transaction(self.conn):
            cursor = self.conn.execute(query, (new_lease_expires_at, now, job_id, lease_token))
            row = cursor.fetchone()
            if row is None:
                msg = f"Lease renewal rejected: token {lease_token} lost for job {job_id}"
                raise LeaseLostError(msg)
            return float(row["lease_expires_at"])

    def release_job(
        self,
        *,
        job_id: str,
        lease_token: str,
        worker_id: str,
        now: float,
    ) -> Job:
        """Release a running job back to queued without incrementing attempts."""
        query = """
        UPDATE jobs
        SET status = 'queued',
            lease_owner = NULL,
            lease_token = NULL,
            lease_expires_at = NULL,
            attempts = attempts - 1,
            updated_at = ?
        WHERE id = ? AND lease_token = ? AND status = 'running'
        RETURNING *;
        """
        with immediate_transaction(self.conn):
            cursor = self.conn.execute(query, (now, job_id, lease_token))
            row = cursor.fetchone()
            if row is None:
                msg = f"Lease lost for job {job_id} during graceful release"
                raise LeaseLostError(msg)

            job = Job.from_row(row)
            self.conn.execute(
                """
                UPDATE attempts
                SET outcome = 'cancelled',
                    finished_at = ?,
                    error = 'Released back to queue on graceful shutdown'
                WHERE id = (
                    SELECT id FROM attempts
                    WHERE job_id = ? AND outcome = 'running' AND worker_id = ?
                    ORDER BY id DESC LIMIT 1
                )
                """,
                (now, job.id, worker_id),
            )
            return job

    def requeue_job(self, *, job_id: str, now: float) -> Job:
        """Requeue a dead job, resetting attempts to 0 while keeping history."""
        # Check current state first to provide precise error if not dead
        job = self.get_job(job_id)
        if job is None:
            msg = f"Job {job_id} not found"
            raise JobNotFoundError(msg)

        if job.status != JobStatus.DEAD:
            from jobqueue.models import InvalidStateTransitionError

            msg = f"Cannot requeue job {job_id} in status '{job.status.value}'. Must be 'dead'."
            raise InvalidStateTransitionError(msg)

        query = """
        UPDATE jobs
        SET status = 'queued',
            attempts = 0,
            run_at = ?,
            lease_owner = NULL,
            lease_token = NULL,
            lease_expires_at = NULL,
            last_error = NULL,
            updated_at = ?
        WHERE id = ? AND status = 'dead'
        RETURNING *;
        """
        with immediate_transaction(self.conn):
            cursor = self.conn.execute(query, (now, now, job_id))
            row = cursor.fetchone()
            if row is None:
                msg = f"Job {job_id} could not be requeued (status changed concurrently)"
                raise JobNotFoundError(msg)
            return Job.from_row(row)

    def get_job(self, job_id: str) -> Job | None:
        """Retrieve a single job by ID."""
        cursor = self.conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        return Job.from_row(row) if row else None

    def list_jobs(
        self,
        *,
        status: JobStatus | None = None,
        queue_name: str | None = None,
        limit: int = 100,
    ) -> list[Job]:
        """List jobs filtered by status and/or queue name."""
        where_clauses: list[str] = []
        params: list[Any] = []

        if status is not None:
            where_clauses.append("status = ?")
            params.append(status.value)
        if queue_name is not None:
            where_clauses.append("queue_name = ?")
            params.append(queue_name)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        query = f"SELECT * FROM jobs {where_sql} ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        cursor = self.conn.execute(query, params)
        return [Job.from_row(row) for row in cursor.fetchall()]

    def get_attempts(self, job_id: str) -> list[Attempt]:
        """Retrieve all attempts for a given job ordered chronologically."""
        cursor = self.conn.execute(
            "SELECT * FROM attempts WHERE job_id = ? ORDER BY id ASC",
            (job_id,),
        )
        return [Attempt.from_row(row) for row in cursor.fetchall()]

    def count_by_status(self, queue_name: str | None = None) -> dict[str, int]:
        """Return counts of jobs grouped by status."""
        counts = {s.value: 0 for s in JobStatus}
        where_sql = "WHERE queue_name = ?" if queue_name else ""
        params = [queue_name] if queue_name else []

        cursor = self.conn.execute(
            f"SELECT status, COUNT(*) as cnt FROM jobs {where_sql} GROUP BY status",
            params,
        )
        for row in cursor.fetchall():
            counts[row["status"]] = int(row["cnt"])
        return counts
