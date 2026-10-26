"""Tests for dashboard HTTP server and REST endpoints."""

from __future__ import annotations

import json
import threading
import urllib.request
from pathlib import Path

from jobqueue.dashboard import create_dashboard_server
from jobqueue.models import JobStatus
from jobqueue.queue import JobQueue


def test_dashboard_server_endpoints(tmp_path: Path) -> None:
    """Dashboard server serves UI and handles REST requests."""
    db_path = tmp_path / "dashboard_test.db"
    q = JobQueue(db_path)

    # Populate jobs: 1 done, 1 dead
    q.enqueue("task_done", {"msg": "done"})
    claim1 = q.claim("w1")
    assert claim1 is not None
    q.ack(claim1.id, claim1.lease_token or "", worker_id="w1")

    job2 = q.enqueue("task_dead", {"msg": "dead"}, max_attempts=1)
    claim2 = q.claim("w2")
    assert claim2 is not None
    q.nack(claim2.id, claim2.lease_token or "", worker_id="w2", error="crash")
    q.close()

    # Start dashboard HTTP server on random free port
    server = create_dashboard_server(db_path, host="127.0.0.1", port=0)
    port = server.server_port
    base_url = f"http://127.0.0.1:{port}"

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    try:
        # 1. GET / (UI)
        with urllib.request.urlopen(f"{base_url}/") as resp:
            assert resp.status == 200
            html = resp.read().decode("utf-8")
            assert "jobqueue dashboard" in html
            assert "Total Jobs" in html

        # 2. GET /api/stats
        with urllib.request.urlopen(f"{base_url}/api/stats") as resp:
            assert resp.status == 200
            stats = json.loads(resp.read().decode("utf-8"))
            assert stats["done"] == 1
            assert stats["dead"] == 1
            assert stats["total"] == 2

        # 3. GET /api/jobs
        with urllib.request.urlopen(f"{base_url}/api/jobs") as resp:
            assert resp.status == 200
            jobs = json.loads(resp.read().decode("utf-8"))
            assert len(jobs) == 2

        # 4. GET /api/jobs/<job_id>
        with urllib.request.urlopen(f"{base_url}/api/jobs/{job2.id}") as resp:
            assert resp.status == 200
            detail = json.loads(resp.read().decode("utf-8"))
            assert detail["job"]["id"] == job2.id
            assert len(detail["attempts"]) == 1
            assert detail["attempts"][0]["error"] == "crash"

        # 5. POST /api/requeue/<job_id>
        req = urllib.request.Request(f"{base_url}/api/requeue/{job2.id}", method="POST")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            res = json.loads(resp.read().decode("utf-8"))
            assert res["success"] is True
            assert res["job"]["status"] == JobStatus.QUEUED.value

        # 6. POST /api/enqueue
        new_job_payload = json.dumps({
            "task_name": "api_task",
            "payload": '{"foo": "bar"}',
            "max_attempts": 5,
        }).encode("utf-8")
        enqueue_req = urllib.request.Request(
            f"{base_url}/api/enqueue",
            data=new_job_payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(enqueue_req) as resp:
            assert resp.status == 201
            res = json.loads(resp.read().decode("utf-8"))
            assert res["success"] is True
            assert res["job"]["task_name"] == "api_task"
            assert res["job"]["max_attempts"] == 5

    finally:
        server.shutdown()
        server.server_close()
