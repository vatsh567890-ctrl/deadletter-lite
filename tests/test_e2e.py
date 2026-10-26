"""End-to-end integration tests running real workers and validating dashboard counts."""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from pathlib import Path

from jobqueue.dashboard import create_dashboard_server
from jobqueue.models import JobStatus
from jobqueue.queue import JobQueue
from jobqueue.worker import WorkerPool


def test_e2e_workers_and_dashboard_counts(tmp_path: Path) -> None:
    """Enqueue succeeding and failing jobs, run real worker pool.

    Assert dashboard counts match DB.
    """
    db_path = tmp_path / "e2e_integration.db"
    q = JobQueue(db_path)

    # 1. Enqueue succeeding jobs
    for i in range(3):
        q.enqueue("echo", {"item": f"success-{i}"})

    # 2. Enqueue failing jobs that will enter dead-letter immediately (max_attempts=1)
    dlq_job_ids = []
    for i in range(2):
        job = q.enqueue("fail_task", {"reason": f"fatal-{i}"}, max_attempts=1)
        dlq_job_ids.append(job.id)

    q.close()

    # 3. Start dashboard HTTP server
    server = create_dashboard_server(db_path, host="127.0.0.1", port=0)
    port = server.server_port
    base_url = f"http://127.0.0.1:{port}"
    host_header = f"127.0.0.1:{port}"

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    try:
        # 4. Run real worker pool with 2 worker processes
        pool = WorkerPool(
            db_path,
            concurrency=2,
            handlers_module="examples.handlers",
            poll_interval=0.05,
        )
        pool.start()

        try:
            # Poll with a 15-second deadline until all jobs have finished processing
            deadline = time.monotonic() + 15.0
            check_q = JobQueue(db_path, read_only=True)
            try:
                while time.monotonic() < deadline:
                    counts = check_q.count_by_status()
                    if counts.get("done", 0) == 3 and counts.get("dead", 0) == 2:
                        break
                    time.sleep(0.1)
            finally:
                check_q.close()
        finally:
            pool.stop()

        # 5. Fetch stats from dashboard API
        stats_req = urllib.request.Request(
            f"{base_url}/api/stats",
            headers={"Host": host_header},
        )
        with urllib.request.urlopen(stats_req) as resp:
            assert resp.status == 200
            dashboard_stats = json.loads(resp.read().decode("utf-8"))

        # 6. Query database directly and compare
        verify_q = JobQueue(db_path, read_only=True)
        try:
            db_counts = verify_q.count_by_status()
            total_db_jobs = sum(db_counts.values())

            # Assert exact match between dashboard JSON counts and database
            assert dashboard_stats["total"] == total_db_jobs == 5
            assert dashboard_stats["done"] == db_counts.get("done", 0) == 3
            assert dashboard_stats["dead"] == db_counts.get("dead", 0) == 2
            assert dashboard_stats["queued"] == db_counts.get("queued", 0) == 0
            assert dashboard_stats["running"] == db_counts.get("running", 0) == 0

            for status_name, count in db_counts.items():
                assert dashboard_stats[status_name] == count

            # 7. Assert jobs listing from dashboard matches DB jobs
            jobs_req = urllib.request.Request(
                f"{base_url}/api/jobs",
                headers={"Host": host_header},
            )
            with urllib.request.urlopen(jobs_req) as resp:
                assert resp.status == 200
                jobs_list = json.loads(resp.read().decode("utf-8"))
                assert len(jobs_list) == total_db_jobs

            done_jobs = [j for j in jobs_list if j["status"] == JobStatus.DONE.value]
            dead_jobs = [j for j in jobs_list if j["status"] == JobStatus.DEAD.value]
            assert len(done_jobs) == 3
            assert len(dead_jobs) == 2
        finally:
            verify_q.close()

        # 8. Test requeue through dashboard API and verify updated counts in DB & stats
        target_requeue_id = dlq_job_ids[0]
        requeue_req = urllib.request.Request(
            f"{base_url}/api/requeue/{target_requeue_id}",
            data=b"",
            headers={
                "Host": host_header,
                "Origin": f"http://{host_header}",
                "X-Requested-With": "jobqueue",
            },
            method="POST",
        )
        with urllib.request.urlopen(requeue_req) as resp:
            assert resp.status == 200
            requeue_res = json.loads(resp.read().decode("utf-8"))
            assert requeue_res["success"] is True

        # Verify updated counts via dashboard /api/stats
        with urllib.request.urlopen(stats_req) as resp:
            updated_stats = json.loads(resp.read().decode("utf-8"))

        verify_q2 = JobQueue(db_path, read_only=True)
        try:
            updated_db_counts = verify_q2.count_by_status()
            assert updated_stats["dead"] == updated_db_counts.get("dead", 0) == 1
            assert updated_stats["queued"] == updated_db_counts.get("queued", 0) == 1
            assert updated_stats["done"] == updated_db_counts.get("done", 0) == 3
            assert updated_stats["total"] == sum(updated_db_counts.values()) == 5
        finally:
            verify_q2.close()

    finally:
        server.shutdown()
        server.server_close()
