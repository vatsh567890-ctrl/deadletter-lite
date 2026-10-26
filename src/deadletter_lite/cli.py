"""Command-line interface (CLI) for deadletter-lite."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from deadletter_lite.dashboard import run_dashboard
from deadletter_lite.models import JobNotFoundError, JobStatus
from deadletter_lite.queue import JobQueue
from deadletter_lite.worker import WorkerPool


def get_default_db() -> str:
    """Return default database path from env or fallback."""
    return os.environ.get("DEADLETTER_DB", os.environ.get("JOBQUEUE_DB", "./deadletter.db"))


def build_parser() -> argparse.ArgumentParser:
    """Construct CLI argument parser with subcommands."""
    parser = argparse.ArgumentParser(
        prog="deadletter-lite",
        description="A lightweight, production-quality job queue backed by SQLite",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- enqueue ---
    enqueue_p = subparsers.add_parser("enqueue", help="Enqueue a new job")
    enqueue_p.add_argument("--task", required=True, help="Task name")
    enqueue_p.add_argument("--payload", required=True, help="JSON payload string")
    enqueue_p.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Maximum execution attempts (default: 3)",
    )
    enqueue_p.add_argument(
        "--queue",
        default="default",
        help="Queue name (default: default)",
    )
    enqueue_p.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay execution by N seconds (default: 0.0)",
    )
    enqueue_p.add_argument(
        "--db",
        default=None,
        help="Database file path (default: $JOBQUEUE_DB or ./jobqueue.db)",
    )

    # --- worker ---
    worker_p = subparsers.add_parser("worker", help="Start worker pool")
    worker_p.add_argument(
        "--db",
        default=None,
        help="Database file path (default: $JOBQUEUE_DB or ./jobqueue.db)",
    )
    worker_p.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Number of worker processes (default: $JOBQUEUE_CONCURRENCY or 1)",
    )
    worker_p.add_argument(
        "--handlers",
        default=None,
        help="Python module containing task handlers (default: $JOBQUEUE_HANDLERS)",
    )
    worker_p.add_argument(
        "--queue",
        default=None,
        help="Filter to a specific queue name",
    )
    worker_p.add_argument(
        "--lease-duration",
        type=float,
        default=30.0,
        help="Lease duration in seconds (default: 30.0)",
    )
    worker_p.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds to wait when queue is idle (default: 1.0)",
    )
    worker_p.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Task handler execution timeout in seconds",
    )

    # --- inspect ---
    inspect_p = subparsers.add_parser("inspect", help="Inspect jobs and statistics")
    inspect_p.add_argument(
        "--db",
        default=None,
        help="Database file path (default: $JOBQUEUE_DB or ./jobqueue.db)",
    )
    inspect_p.add_argument(
        "--status",
        choices=["queued", "running", "done", "dead"],
        default=None,
        help="Filter by status",
    )
    inspect_p.add_argument(
        "--queue",
        default=None,
        help="Filter by queue name",
    )
    inspect_p.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum jobs to list (default: 20)",
    )
    inspect_p.add_argument(
        "--job-id",
        default=None,
        help="Inspect a specific job ID and its attempt history",
    )

    # --- requeue ---
    requeue_p = subparsers.add_parser("requeue", help="Requeue a dead-letter job")
    requeue_p.add_argument("job_id", help="ID of dead job to requeue")
    requeue_p.add_argument(
        "--db",
        default=None,
        help="Database file path (default: $JOBQUEUE_DB or ./jobqueue.db)",
    )

    # --- dashboard ---
    dash_p = subparsers.add_parser("dashboard", help="Start local web dashboard")
    dash_p.add_argument(
        "--db",
        default=None,
        help="Database file path (default: $JOBQUEUE_DB or ./jobqueue.db)",
    )
    dash_p.add_argument(
        "--host",
        default=None,
        help="Host address to bind (default: $JOBQUEUE_HOST or 127.0.0.1)",
    )
    dash_p.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to listen on (default: $JOBQUEUE_PORT or 8080)",
    )

    return parser


def handle_enqueue(args: argparse.Namespace) -> int:
    """Execute enqueue command."""
    db_path = args.db or get_default_db()
    q = JobQueue(db_path)
    try:
        job = q.enqueue(
            task_name=args.task,
            payload=args.payload,
            max_attempts=args.max_attempts,
            queue_name=args.queue,
            delay=args.delay,
        )
        print(f"Enqueued job {job.id} for task '{job.task_name}' in queue '{job.queue_name}'")
        return 0
    except Exception as err:
        print(f"Error enqueuing job: {err}", file=sys.stderr)
        return 1
    finally:
        q.close()


def handle_worker(args: argparse.Namespace) -> int:
    """Execute worker pool command."""
    db_path = args.db or get_default_db()
    concurrency_env = os.environ.get("JOBQUEUE_CONCURRENCY")
    concurrency = args.concurrency or (int(concurrency_env) if concurrency_env else 1)

    handlers = args.handlers or os.environ.get("JOBQUEUE_HANDLERS")
    pool = WorkerPool(
        db_path=db_path,
        concurrency=concurrency,
        handlers_module=handlers,
        queue_name=args.queue,
        lease_duration=args.lease_duration,
        poll_interval=args.poll_interval,
        timeout=args.timeout,
    )
    print(f"Starting WorkerPool with {concurrency} workers (db: {db_path})")
    pool.start()
    try:
        pool.wait()
    except KeyboardInterrupt:
        print("\nShutting down workers...")
        pool.stop()
    return 0


def handle_inspect(args: argparse.Namespace) -> int:
    """Execute inspect command."""
    db_path = args.db or get_default_db()
    if not Path(db_path).exists():
        print(f"Database file does not exist: {db_path}", file=sys.stderr)
        return 1

    q = JobQueue(db_path, read_only=True)
    try:
        if args.job_id:
            job = q.get(args.job_id)
            if job is None:
                print(f"Job '{args.job_id}' not found", file=sys.stderr)
                return 1
            print(f"Job ID:       {job.id}")
            print(f"Task Name:    {job.task_name}")
            print(f"Queue:        {job.queue_name}")
            print(f"Status:       {job.status.value}")
            print(f"Attempts:     {job.attempts} / {job.max_attempts}")
            print(f"Payload:      {job.payload}")
            print(f"Last Error:   {job.last_error}")
            print(f"Created At:   {job.created_at}")
            print(f"Updated At:   {job.updated_at}")

            attempts = q.get_attempts(job.id)
            print(f"\nAttempts ({len(attempts)}):")
            for att in attempts:
                err_str = f" - Error: {att.error}" if att.error else ""
                print(
                    f"  #{att.attempt_number} [{att.outcome.value}] "
                    f"by {att.worker_id} at {att.started_at}{err_str}"
                )
            return 0

        # List jobs
        status_enum = JobStatus(args.status) if args.status else None
        jobs = q.list_jobs(status=status_enum, queue_name=args.queue, limit=args.limit)
        counts = q.count_by_status(args.queue)

        print(
            f"Counts: Queued={counts['queued']}, Running={counts['running']}, "
            f"Done={counts['done']}, Dead={counts['dead']}"
        )
        print("-" * 75)
        print(f"{'ID':<10} {'TASK':<18} {'STATUS':<10} {'ATTEMPTS':<10} {'QUEUE':<10} LAST ERROR")
        print("-" * 75)
        for job in jobs:
            err = ""
            if job.last_error:
                err = job.last_error[:20] + "..." if len(job.last_error) > 20 else job.last_error
            print(
                f"{job.id[:8]:<10} {job.task_name[:17]:<18} {job.status.value:<10} "
                f"{job.attempts}/{job.max_attempts:<8} {job.queue_name[:9]:<10} {err}"
            )
        return 0
    finally:
        q.close()


def handle_requeue(args: argparse.Namespace) -> int:
    """Execute requeue command."""
    db_path = args.db or get_default_db()
    q = JobQueue(db_path)
    try:
        job = q.requeue(args.job_id)
        print(f"Requeued dead job {job.id} back to queued status")
        return 0
    except JobNotFoundError:
        print(f"Job '{args.job_id}' not found", file=sys.stderr)
        return 1
    except Exception as err:
        print(f"Could not requeue job: {err}", file=sys.stderr)
        return 1
    finally:
        q.close()


def handle_dashboard(args: argparse.Namespace) -> int:
    """Execute dashboard command."""
    db_path = args.db or get_default_db()
    host = args.host or os.environ.get("JOBQUEUE_HOST", "127.0.0.1")
    port_env = os.environ.get("JOBQUEUE_PORT")
    port = args.port or (int(port_env) if port_env else 8080)

    print(f"Starting dashboard at http://{host}:{port} (db: {db_path})")
    run_dashboard(db_path, host=host, port=port)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Main CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "enqueue":
        return handle_enqueue(args)
    if args.command == "worker":
        return handle_worker(args)
    if args.command == "inspect":
        return handle_inspect(args)
    if args.command == "requeue":
        return handle_requeue(args)
    if args.command == "dashboard":
        return handle_dashboard(args)

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
