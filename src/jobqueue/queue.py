"""High-level Queue API managing job lifecycle and state transitions."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from jobqueue.backoff import RngProtocol, compute_backoff
from jobqueue.clock import Clock, SystemClock
from jobqueue.models import (
    Attempt,
    AttemptOutcome,
    InvalidStateTransitionError,
    Job,
    JobNotFoundError,
    JobStatus,
    LeaseLostError,
)
from jobqueue.storage import Storage


class JobQueue:
    """High-level queue interface encapsulating durable operations and state transitions."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        clock: Clock | None = None,
        rng: RngProtocol | None = None,
        backoff_base: float = 1.0,
        max_backoff: float = 60.0,
        timeout: float = 5.0,
        read_only: bool = False,
    ) -> None:
        self.db_path = Path(db_path)
        self.clock: Clock = clock or SystemClock()
        self.rng: RngProtocol | None = rng
        self.backoff_base = backoff_base
        self.max_backoff = max_backoff
        self.storage = Storage.open(self.db_path, timeout=timeout, read_only=read_only)

    def close(self) -> None:
        """Close the storage connection."""
        self.storage.close()

    def enqueue(
        self,
        task_name: str,
        payload: str | dict[str, object] | list[object],
        *,
        max_attempts: int = 3,
        queue_name: str = "default",
        delay: float = 0.0,
        job_id: str | None = None,
    ) -> Job:
        """Enqueue a new job with a JSON payload."""
        now = self.clock.now()
        run_at = now + max(0.0, delay)

        if not isinstance(payload, str):
            payload_str = json.dumps(payload)
        else:
            # Validate that string is valid JSON
            try:
                json.loads(payload)
                payload_str = payload
            except json.JSONDecodeError as err:
                msg = f"Payload must be valid JSON: {err}"
                raise ValueError(msg) from err

        assigned_id = job_id or str(uuid.uuid4())
        return self.storage.insert_job(
            job_id=assigned_id,
            queue_name=queue_name,
            task_name=task_name,
            payload=payload_str,
            max_attempts=max_attempts,
            run_at=run_at,
            created_at=now,
        )

    def claim(
        self,
        worker_id: str,
        *,
        lease_duration: float = 30.0,
        queue_name: str | None = None,
    ) -> Job | None:
        """Atomically claim the next eligible job, assigning a new UUID lease token."""
        now = self.clock.now()
        lease_token = str(uuid.uuid4())
        return self.storage.claim_job(
            worker_id=worker_id,
            lease_token=lease_token,
            lease_duration=lease_duration,
            now=now,
            queue_name=queue_name,
        )

    def ack(
        self,
        job_id: str,
        lease_token: str,
        *,
        worker_id: str,
    ) -> Job:
        """Acknowledge job completion under fencing protection."""
        now = self.clock.now()
        # Verify job exists and check state transition
        existing = self.storage.get_job(job_id)
        if existing is None:
            msg = f"Job {job_id} not found"
            raise JobNotFoundError(msg)
        if existing.status != JobStatus.RUNNING:
            msg = (
                f"Cannot ack job {job_id} with status '{existing.status.value}'. "
                f"Must be 'running'."
            )
            raise InvalidStateTransitionError(msg)
        if existing.lease_token != lease_token:
            msg = f"Lease token mismatch for job {job_id}."
            raise LeaseLostError(msg)

        return self.storage.ack_job(
            job_id=job_id,
            lease_token=lease_token,
            worker_id=worker_id,
            now=now,
        )

    def nack(
        self,
        job_id: str,
        lease_token: str,
        *,
        worker_id: str,
        error: str,
        outcome: AttemptOutcome = AttemptOutcome.FAILED,
        force_dead: bool = False,
    ) -> Job:
        """Nack a job, retrying with backoff if attempts remain, or moving to dead-letter."""
        now = self.clock.now()
        existing = self.storage.get_job(job_id)
        if existing is None:
            msg = f"Job {job_id} not found"
            raise JobNotFoundError(msg)
        if existing.status != JobStatus.RUNNING:
            msg = (
                f"Cannot nack job {job_id} with status '{existing.status.value}'. "
                f"Must be 'running'."
            )
            raise InvalidStateTransitionError(msg)
        if existing.lease_token != lease_token:
            msg = f"Lease token mismatch for job {job_id}."
            raise LeaseLostError(msg)

        # Determine whether retry is available
        if force_dead or existing.attempts >= existing.max_attempts:
            retry_run_at = None
        else:
            delay = compute_backoff(
                attempt=existing.attempts,
                base=self.backoff_base,
                max_backoff=self.max_backoff,
                rng=self.rng,
            )
            retry_run_at = now + delay

        return self.storage.nack_job(
            job_id=job_id,
            lease_token=lease_token,
            worker_id=worker_id,
            error=error,
            outcome=outcome,
            retry_run_at=retry_run_at,
            now=now,
        )

    def renew(
        self,
        job_id: str,
        lease_token: str,
        *,
        lease_duration: float = 30.0,
    ) -> float:
        """Renew active lease for a running job."""
        now = self.clock.now()
        return self.storage.renew_lease(
            job_id=job_id,
            lease_token=lease_token,
            lease_duration=lease_duration,
            now=now,
        )

    def release(
        self,
        job_id: str,
        lease_token: str,
        *,
        worker_id: str,
    ) -> Job:
        """Release a claimed job back to queued without incrementing attempts."""
        now = self.clock.now()
        return self.storage.release_job(
            job_id=job_id,
            lease_token=lease_token,
            worker_id=worker_id,
            now=now,
        )

    def requeue(self, job_id: str) -> Job:
        """Requeue a dead job back to queued state, resetting attempts to 0."""
        now = self.clock.now()
        return self.storage.requeue_job(job_id=job_id, now=now)

    def get(self, job_id: str) -> Job | None:
        """Retrieve a job by ID."""
        return self.storage.get_job(job_id)

    def list_jobs(
        self,
        *,
        status: JobStatus | None = None,
        queue_name: str | None = None,
        limit: int = 100,
    ) -> list[Job]:
        """List jobs with optional filtering."""
        return self.storage.list_jobs(status=status, queue_name=queue_name, limit=limit)

    def get_attempts(self, job_id: str) -> list[Attempt]:
        """Retrieve attempt history for a job."""
        return self.storage.get_attempts(job_id)

    def count_by_status(self, queue_name: str | None = None) -> dict[str, int]:
        """Count jobs by status."""
        return self.storage.count_by_status(queue_name)
