"""Tests for dashboard HTTP server, security controls, and REST endpoints."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Generator
from pathlib import Path

import pytest

from jobqueue.dashboard import create_dashboard_server
from jobqueue.models import JobStatus
from jobqueue.queue import JobQueue


@pytest.fixture
def running_server(tmp_path: Path) -> Generator[tuple[str, Path, int], None, None]:
    """Start dashboard HTTP server on random port and yield base_url, db_path, port."""
    db_path = tmp_path / "dashboard_test.db"
    server = create_dashboard_server(db_path, host="127.0.0.1", port=0)
    port = server.server_port
    base_url = f"http://127.0.0.1:{port}"

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    yield base_url, db_path, port

    server.shutdown()
    server.server_close()


def test_dashboard_server_endpoints(running_server: tuple[str, Path, int]) -> None:
    """Dashboard server serves UI and handles REST requests with security headers."""
    base_url, db_path, _ = running_server
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

    # 1. GET / (UI)
    with urllib.request.urlopen(f"{base_url}/") as resp:
        assert resp.status == 200
        html = resp.read().decode("utf-8")
        assert "jobqueue dashboard" in html
        assert "Total Jobs" in html
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"
        assert "Content-Security-Policy" in resp.headers

    # 2. GET /api/stats
    with urllib.request.urlopen(f"{base_url}/api/stats") as resp:
        assert resp.status == 200
        stats = json.loads(resp.read().decode("utf-8"))
        assert stats["done"] == 1
        assert stats["dead"] == 1
        assert stats["total"] == 2
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"
        assert "default-src 'self'" in resp.headers.get("Content-Security-Policy", "")

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
    req = urllib.request.Request(
        f"{base_url}/api/requeue/{job2.id}",
        headers={"X-Requested-With": "jobqueue"},
        method="POST",
    )
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
        headers={
            "Content-Type": "application/json",
            "X-Requested-With": "jobqueue",
        },
        method="POST",
    )
    with urllib.request.urlopen(enqueue_req) as resp:
        assert resp.status == 201
        res = json.loads(resp.read().decode("utf-8"))
        assert res["success"] is True
        assert res["job"]["task_name"] == "api_task"
        assert res["job"]["max_attempts"] == 5


def test_dashboard_foreign_host_rejected(running_server: tuple[str, Path, int]) -> None:
    """Request with foreign Host header is rejected with 400 or 403."""
    base_url, _, _ = running_server

    req = urllib.request.Request(
        f"{base_url}/api/stats",
        headers={"Host": "attacker.example.com"},
    )
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req)
    assert exc_info.value.code in (400, 403)


def test_dashboard_cross_origin_post_rejected(running_server: tuple[str, Path, int]) -> None:
    """POST to mutating endpoint from foreign Origin is rejected with 403."""
    base_url, _, _ = running_server

    payload = json.dumps({"task_name": "evil_task", "payload": "{}"}).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/api/enqueue",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Requested-With": "jobqueue",
            "Origin": "http://evil-site.com",
        },
        method="POST",
    )
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req)
    assert exc_info.value.code == 403


def test_dashboard_post_without_custom_header_rejected(
    running_server: tuple[str, Path, int],
) -> None:
    """POST to mutating endpoint without X-Requested-With: jobqueue is rejected with 403."""
    base_url, _, _ = running_server

    payload = json.dumps({"task_name": "unauthenticated", "payload": "{}"}).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/api/enqueue",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req)
    assert exc_info.value.code in (400, 403)


def test_dashboard_oversized_body_returns_413(running_server: tuple[str, Path, int]) -> None:
    """Request with body larger than 16 KB is rejected with 413 Payload Too Large."""
    base_url, _, _ = running_server

    # Create oversized body > 16 KB (e.g. 20 KB)
    large_payload = json.dumps({"data": "x" * (20 * 1024)}).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/api/enqueue",
        data=large_payload,
        headers={
            "Content-Type": "application/json",
            "X-Requested-With": "jobqueue",
        },
        method="POST",
    )
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req)
    assert exc_info.value.code == 413


def test_dashboard_xss_job_name_escaped(running_server: tuple[str, Path, int]) -> None:
    """A job named <img src=x onerror=alert(1)> is safely escaped and does not execute."""
    base_url, db_path, _ = running_server
    q = JobQueue(db_path)

    xss_task = "<img src=x onerror=alert(1)>"
    xss_payload = '{"test": "<script>alert(2)</script>"}'
    job = q.enqueue(xss_task, xss_payload)
    q.close()

    # Query API to ensure JSON data is accurately stored
    with urllib.request.urlopen(f"{base_url}/api/jobs/{job.id}") as resp:
        assert resp.status == 200
        detail = json.loads(resp.read().decode("utf-8"))
        assert detail["job"]["task_name"] == xss_task

    # Load UI HTML and verify raw task string is not hardcoded as raw unescaped markup
    with urllib.request.urlopen(f"{base_url}/") as resp:
        assert resp.status == 200
        html = resp.read().decode("utf-8")
        assert "<img src=x onerror=alert(1)>" not in html
        assert "<script>alert(2)</script>" not in html
        assert "Content-Security-Policy" in resp.headers


def test_dashboard_new_db_path_creates_schema(tmp_path: Path) -> None:
    """Dashboard works on a path that does not exist yet and creates schema."""
    non_existent_db = tmp_path / "subdir" / "new_db.db"
    assert not non_existent_db.exists()

    server = create_dashboard_server(non_existent_db, host="127.0.0.1", port=0)
    try:
        assert non_existent_db.exists()
        # Verify schema exists by querying via JobQueue
        q = JobQueue(non_existent_db)
        counts = q.count_by_status()
        assert counts["queued"] == 0
        q.close()
    finally:
        server.server_close()
