"""Worker implementation and multi-process execution pool for jobqueue."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import multiprocessing as mp
import os
import signal
import sys
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from jobqueue.clock import Clock, SystemClock
from jobqueue.models import (
    AttemptOutcome,
    HandlerTimeoutError,
    Job,
    LeaseLostError,
    TaskNotFoundError,
)
from jobqueue.queue import JobQueue
from jobqueue.registry import Registry, default_registry

logger = logging.getLogger("jobqueue.worker")


class Worker:
    """Single worker instance managing job execution, leasing, and heartbeats."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        worker_id: str | None = None,
        registry: Registry | None = None,
        lease_duration: float = 30.0,
        poll_interval: float = 1.0,
        queue_name: str | None = None,
        timeout: float | None = None,
        clock: Clock | None = None,
        stop_event: threading.Event | Any = None,
    ) -> None:
        self.db_path = Path(db_path)
        self.worker_id = worker_id or f"worker-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.registry = registry or default_registry
        self.lease_duration = lease_duration
        self.poll_interval = poll_interval
        self.queue_name = queue_name
        self.timeout = timeout
        self.clock = clock or SystemClock()
        self.stop_event = stop_event or threading.Event()
        self._current_job: Job | None = None
        self._current_lease_token: str | None = None

    def execute_handler(self, handler: Callable[..., Any], payload: str) -> Any:
        """Execute a task handler with optional payload parsing and timeout."""

        def run_fn() -> Any:
            try:
                return handler(payload)
            except TypeError:
                # If handler expects parsed JSON dictionary/list
                try:
                    parsed = json.loads(payload)
                    return handler(parsed)
                except Exception:
                    raise

        if self.timeout is not None and self.timeout > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run_fn)
                try:
                    return future.result(timeout=self.timeout)
                except TimeoutError as err:
                    msg = f"Task handler exceeded timeout of {self.timeout}s"
                    raise HandlerTimeoutError(msg) from err
        else:
            return run_fn()

    def process_job(self, q: JobQueue, job: Job) -> bool:
        """Process a single claimed job with active heartbeat lease renewal."""
        lease_token = job.lease_token
        if lease_token is None:
            return False

        self._current_job = job
        self._current_lease_token = lease_token

        # Heartbeat thread for lease renewal with dedicated connection
        heartbeat_stop = threading.Event()
        renew_interval = max(0.1, self.lease_duration / 3.0)

        def heartbeat_loop() -> None:
            renew_q = JobQueue(self.db_path, clock=self.clock)
            try:
                while not heartbeat_stop.wait(timeout=renew_interval):
                    try:
                        renew_q.renew(job.id, lease_token, lease_duration=self.lease_duration)
                        logger.debug("Renewed lease for job %s", job.id)
                    except LeaseLostError:
                        logger.warning("Heartbeat lease renewal lost for job %s", job.id)
                        break
                    except Exception as ex:
                        logger.debug("Error during heartbeat renewal: %s", ex)
                        break
            finally:
                renew_q.close()

        heartbeat_thread = threading.Thread(
            target=heartbeat_loop,
            daemon=True,
            name=f"heartbeat-{job.id[:8]}",
        )
        heartbeat_thread.start()

        try:
            # Check registry for handler
            if not self.registry.contains(job.task_name):
                msg = f"Unknown task '{job.task_name}'. Handler is not registered."
                raise TaskNotFoundError(msg)

            handler = self.registry.get(job.task_name)
            logger.info(
                "Worker %s executing task %s (job %s)",
                self.worker_id,
                job.task_name,
                job.id,
            )
            self.execute_handler(handler, job.payload)

            # Handler completed
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=1.0)

            if self.stop_event.is_set():
                q.release(job.id, lease_token, worker_id=self.worker_id)
                logger.info("Released job %s back to queue on shutdown", job.id)
                return False

            q.ack(job.id, lease_token, worker_id=self.worker_id)
            logger.info("Job %s acknowledged successfully", job.id)
            return True

        except TaskNotFoundError as err:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=1.0)
            logger.error("Job %s failed: task not found: %s", job.id, err)
            try:
                q.nack(
                    job.id,
                    lease_token,
                    worker_id=self.worker_id,
                    error=str(err),
                    outcome=AttemptOutcome.FAILED,
                    force_dead=True,
                )
            except LeaseLostError:
                logger.warning("Lease lost while nacking job %s", job.id)
            return False

        except HandlerTimeoutError as err:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=1.0)
            logger.error("Job %s timed out: %s", job.id, err)
            try:
                q.nack(
                    job.id,
                    lease_token,
                    worker_id=self.worker_id,
                    error=str(err),
                    outcome=AttemptOutcome.TIMEOUT,
                )
            except LeaseLostError:
                logger.warning("Lease lost while nacking timed out job %s", job.id)
            return False

        except Exception as err:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=1.0)
            logger.error("Job %s raised exception: %s", job.id, err)
            try:
                q.nack(
                    job.id,
                    lease_token,
                    worker_id=self.worker_id,
                    error=str(err),
                    outcome=AttemptOutcome.FAILED,
                )
            except LeaseLostError:
                logger.warning("Lease lost while nacking failed job %s", job.id)
            return False

        finally:
            heartbeat_stop.set()
            self._current_job = None
            self._current_lease_token = None

    def run_once(self, q: JobQueue | None = None) -> Job | None:
        """Attempt to claim and execute a single job."""
        should_close = False
        if q is None:
            q = JobQueue(self.db_path, clock=self.clock)
            should_close = True

        try:
            job = q.claim(
                worker_id=self.worker_id,
                lease_duration=self.lease_duration,
                queue_name=self.queue_name,
            )
            if job is not None:
                self.process_job(q, job)
            return job
        finally:
            if should_close:
                q.close()

    def run(self, max_jobs: int | None = None) -> int:
        """Run the worker polling loop until stopped or max_jobs processed."""
        q = JobQueue(self.db_path, clock=self.clock)
        jobs_processed = 0

        logger.info("Worker %s started loop on db %s", self.worker_id, self.db_path)
        try:
            while not self.stop_event.is_set():
                if max_jobs is not None and jobs_processed >= max_jobs:
                    break

                job = q.claim(
                    worker_id=self.worker_id,
                    lease_duration=self.lease_duration,
                    queue_name=self.queue_name,
                )
                if job is None:
                    # Sleep respecting stop_event
                    self.stop_event.wait(timeout=self.poll_interval)
                    continue

                self.process_job(q, job)
                jobs_processed += 1

        finally:
            # If stopped while holding a claimed job, release it back to queue
            if self._current_job is not None and self._current_lease_token is not None:
                try:
                    q.release(
                        self._current_job.id,
                        self._current_lease_token,
                        worker_id=self.worker_id,
                    )
                    logger.info("Released job %s back to queue on shutdown", self._current_job.id)
                except Exception as err:
                    logger.debug("Could not release job on shutdown: %s", err)
            q.close()
            logger.info("Worker %s stopped. Total processed: %d", self.worker_id, jobs_processed)

        return jobs_processed


def _worker_process_target(
    db_path: str,
    worker_id: str,
    handlers_module: str | None,
    queue_name: str | None,
    lease_duration: float,
    poll_interval: float,
    timeout: float | None,
    stop_event: Any,
) -> None:
    """Entry point for a spawned child worker process."""

    # Setup signal handlers in child process
    def handle_signal(sig: int, frame: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())

    registry: Registry
    if handlers_module:
        try:
            registry = Registry.from_module(handlers_module)
        except Exception as err:
            logger.error("Failed to import handlers from '%s': %s", handlers_module, err)
            sys.exit(1)
    else:
        registry = default_registry

    worker = Worker(
        db_path=db_path,
        worker_id=worker_id,
        registry=registry,
        lease_duration=lease_duration,
        poll_interval=poll_interval,
        queue_name=queue_name,
        timeout=timeout,
        stop_event=stop_event,
    )
    worker.run()


class WorkerPool:
    """Manages a pool of worker child processes."""

    def __init__(
        self,
        db_path: str | Path,
        concurrency: int = 1,
        *,
        handlers_module: str | None = None,
        queue_name: str | None = None,
        lease_duration: float = 30.0,
        poll_interval: float = 1.0,
        timeout: float | None = None,
    ) -> None:
        self.db_path = str(Path(db_path).resolve())
        self.concurrency = max(1, concurrency)
        self.handlers_module = handlers_module
        self.queue_name = queue_name
        self.lease_duration = lease_duration
        self.poll_interval = poll_interval
        self.timeout = timeout

        self.ctx = mp.get_context("spawn")
        self.stop_event = self.ctx.Event()
        self.processes: list[Any] = []

    def start(self) -> None:
        """Start all worker child processes."""
        for i in range(self.concurrency):
            worker_id = f"worker-pool-{os.getpid()}-{i + 1}"
            p = self.ctx.Process(
                target=_worker_process_target,
                args=(
                    self.db_path,
                    worker_id,
                    self.handlers_module,
                    self.queue_name,
                    self.lease_duration,
                    self.poll_interval,
                    self.timeout,
                    self.stop_event,
                ),
            )
            p.start()
            self.processes.append(p)
        logger.info("WorkerPool started with %d workers", len(self.processes))

    def stop(self, timeout: float = 5.0) -> None:
        """Signal all workers to stop and wait for them to exit."""
        self.stop_event.set()
        for p in self.processes:
            p.join(timeout=timeout)
            if p.is_alive():
                p.terminate()
                p.join(timeout=2.0)
        self.processes.clear()
        logger.info("WorkerPool stopped")

    def wait(self) -> None:
        """Wait for worker processes until interrupted."""
        try:
            while any(p.is_alive() for p in self.processes):
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.stop()
