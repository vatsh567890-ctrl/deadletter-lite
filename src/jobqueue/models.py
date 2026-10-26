"""Domain models and exceptions for jobqueue."""

from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

MAX_PAYLOAD_SIZE = 64 * 1024  # 64 KB cap per job payload


class JobStatus(str, Enum):
    """Job lifecycle statuses."""

    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    DEAD = "dead"


class AttemptOutcome(str, Enum):
    """Outcomes recorded for each execution attempt."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    LEASE_EXPIRED = "lease_expired"
    CANCELLED = "cancelled"


class JobQueueError(Exception):
    """Base exception for all jobqueue errors."""


class InvalidStateTransitionError(JobQueueError):
    """Raised when an illegal lifecycle transition is attempted."""


class LeaseLostError(JobQueueError):
    """Raised when an operation is rejected due to a stale or invalid lease token."""


class JobNotFoundError(JobQueueError):
    """Raised when a specified job does not exist."""


class PayloadTooLargeError(JobQueueError):
    """Raised when a job payload exceeds the allowed size cap."""


class TaskNotFoundError(JobQueueError):
    """Raised when a task name is not registered in the handler registry."""


class HandlerTimeoutError(JobQueueError):
    """Raised when a task handler exceeds its execution timeout."""


@dataclass(frozen=True)
class Job:
    """Represents a job record in durable storage."""

    id: str
    queue_name: str
    task_name: str
    payload: str
    status: JobStatus
    attempts: int
    max_attempts: int
    run_at: float
    lease_owner: str | None
    lease_token: str | None
    lease_expires_at: float | None
    created_at: float
    updated_at: float
    last_error: str | None = None

    def is_retrying(self, current_time: float) -> bool:
        """Check if job is in logical retrying state (queued with future run_at)."""
        return self.status == JobStatus.QUEUED and self.run_at > current_time

    def to_dict(self) -> dict[str, Any]:
        """Convert job to dictionary."""
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Job:
        """Construct a Job model from an SQLite Row."""
        return cls(
            id=str(row["id"]),
            queue_name=str(row["queue_name"]),
            task_name=str(row["task_name"]),
            payload=str(row["payload"]),
            status=JobStatus(row["status"]),
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            run_at=float(row["run_at"]),
            lease_owner=row["lease_owner"],
            lease_token=row["lease_token"],
            lease_expires_at=(
                float(row["lease_expires_at"]) if row["lease_expires_at"] is not None else None
            ),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            last_error=row["last_error"],
        )


@dataclass(frozen=True)
class Attempt:
    """Represents a job execution attempt record."""

    id: int | None
    job_id: str
    attempt_number: int
    worker_id: str
    started_at: float
    finished_at: float | None
    outcome: AttemptOutcome
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert attempt to dictionary."""
        d = asdict(self)
        d["outcome"] = self.outcome.value
        return d

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Attempt:
        """Construct an Attempt model from an SQLite Row."""
        return cls(
            id=int(row["id"]) if "id" in row.keys() and row["id"] is not None else None,
            job_id=str(row["job_id"]),
            attempt_number=int(row["attempt_number"]),
            worker_id=str(row["worker_id"]),
            started_at=float(row["started_at"]),
            finished_at=(
                float(row["finished_at"]) if row["finished_at"] is not None else None
            ),
            outcome=AttemptOutcome(row["outcome"]),
            error=row["error"],
        )
