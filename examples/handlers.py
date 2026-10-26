"""Example task handlers for jobqueue demonstrating usage."""

from __future__ import annotations

import json
import time
from typing import Any

from jobqueue.registry import task

_FLAKY_COUNTER: dict[str, int] = {}


@task("echo")
def echo_task(payload: Any) -> dict[str, Any]:
    """Echo task that logs and returns payload content."""
    data = json.loads(payload) if isinstance(payload, str) else payload
    print(f"[echo_task] Received payload: {data}")
    return {"status": "ok", "echo": data}


@task("fail_task")
def fail_task(payload: Any) -> None:
    """Failing task that always raises an exception to test retry/DLQ behavior."""
    data = json.loads(payload) if isinstance(payload, str) else payload
    reason = data.get("reason", "Simulated task failure") if isinstance(data, dict) else "Error"
    raise RuntimeError(f"fail_task intentional error: {reason}")


@task("flaky_task")
def flaky_task(payload: Any) -> str:
    """Flaky task that fails twice before succeeding on attempt 3."""
    data = json.loads(payload) if isinstance(payload, str) else payload
    job_key = str(data.get("key", "default")) if isinstance(data, dict) else "default"
    count = _FLAKY_COUNTER.get(job_key, 0) + 1
    _FLAKY_COUNTER[job_key] = count
    if count < 3:
        raise ValueError(f"Flaky transient error (count={count})")
    return f"Success on attempt {count}"


@task("slow_task")
def slow_task(payload: Any) -> str:
    """Slow task that sleeps to demonstrate heartbeat lease renewal."""
    data = json.loads(payload) if isinstance(payload, str) else payload
    duration = float(data.get("duration", 2.0)) if isinstance(data, dict) else 2.0
    time.sleep(duration)
    return f"Completed after {duration}s"
