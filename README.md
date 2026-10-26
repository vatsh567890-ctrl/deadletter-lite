# jobqueue

A lightweight, production-quality job queue in Python 3.11+ providing durable SQLite storage, worker processes with retries and exponential backoff, a dead-letter queue, a CLI, and a local dashboard.

Author: vatsh567890-ctrl

## Architecture & Single-Machine Environment
`jobqueue` runs entirely on a single machine. It uses the Python standard library and SQLite in Write-Ahead Logging (WAL) mode for high-concurrency, low-overhead job execution without external daemon requirements like Redis or Celery.

## Delivery Semantics
- **At-least-once execution**: A job can execute more than once if a worker process crashes or terminates after performing work but prior to acknowledging completion to the database. Handlers must be idempotent. Exactly-once execution is not claimed.
- **Ordering**: Ordering is not guaranteed across jobs.
- **Priority**: Priority scheduling is out of scope for v1.

## Storage Design
- **Engine**: SQLite in WAL mode with `busy_timeout` set to 5000ms and `synchronous = NORMAL`.
- **Connection Isolation**: Each process opens its own dedicated connection. Database connections are never shared across processes or multiprocessing forks.
- **Atomic Claiming**: Claiming is executed via a single atomic `UPDATE ... WHERE id = (SELECT id FROM jobs WHERE ... LIMIT 1) RETURNING ...` statement. Two workers can never receive the same job or lease token.
- **Fencing**: Every mutation (`ack`, `nack`, lease renewal, release) checks both the job ID and the current `lease_token`. If another worker reclaimed the job after lease expiration, mutations from the stale worker are rejected with `LeaseLostError`.
- **Parameterized SQL**: All SQL operations use parameter substitution. No user-supplied values are interpolated into queries.

## Installation

```bash
pip install -e .
```

For development:
```bash
pip install -e ".[dev]"
```

## CLI Usage

```bash
# Enqueue a task
jobqueue enqueue --task echo --payload '{"message": "hello"}' --max-attempts 3

# Start worker processes
jobqueue worker --db ./jobqueue.db --concurrency 4 --handlers examples.handlers

# Inspect jobs
jobqueue inspect --status queued --limit 20

# Requeue a dead job
jobqueue requeue <JOB_ID>

# Start local dashboard
jobqueue dashboard --db ./jobqueue.db --port 8080
```

## Running Tests

```bash
# Run fast test suite
pytest -m "not slow"

# Run full test suite including slow stress and crash recovery tests
pytest
```
