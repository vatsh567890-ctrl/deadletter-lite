"""Local web dashboard and REST API for inspecting and managing jobqueue."""

from __future__ import annotations

import json
import logging
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

from jobqueue.models import InvalidStateTransitionError, JobNotFoundError, JobStatus
from jobqueue.queue import JobQueue

logger = logging.getLogger("jobqueue.dashboard")


def get_dashboard_html() -> str:
    """Load the dashboard HTML UI template."""
    html_path = Path(__file__).parent / "dashboard.html"
    if html_path.exists():
        return html_path.read_text(encoding="utf-8")
    return "<html><body><h1>jobqueue dashboard</h1><p>dashboard.html not found</p></body></html>"


class DashboardRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler providing REST endpoints and serving the dashboard."""

    def __init__(self, *args: Any, db_path: Path, **kwargs: Any) -> None:
        self.db_path = db_path
        super().__init__(*args, **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        """Override default logging to suppress routine HTTP access noise."""
        logger.debug(format, *args)

    def _send_json(self, data: Any, status: int = 200) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str, status: int = 200) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _validate_host(self) -> bool:
        """Reject any request whose Host header is not localhost:PORT or 127.0.0.1:PORT."""
        host_header = self.headers.get("Host", "").strip()
        if not host_header:
            self._send_json({"error": "Missing Host header"}, status=400)
            return False

        server_port = getattr(self.server, "server_port", 8080)
        allowed = {
            f"localhost:{server_port}",
            f"127.0.0.1:{server_port}",
        }
        if server_port in (80, 443):
            allowed.update({"localhost", "127.0.0.1"})

        if host_header not in allowed:
            self._send_json({"error": f"Invalid Host header: {host_header}"}, status=403)
            return False
        return True

    def _validate_csrf_and_origin(self) -> bool:
        """Enforce X-Requested-With: jobqueue and reject any non-local Origin."""
        x_req = self.headers.get("X-Requested-With")
        if x_req != "jobqueue":
            self._send_json(
                {"error": "Forbidden: Missing or invalid X-Requested-With header"},
                status=403,
            )
            return False

        origin = self.headers.get("Origin")
        if origin:
            server_port = getattr(self.server, "server_port", 8080)
            allowed = {
                f"http://localhost:{server_port}",
                f"http://127.0.0.1:{server_port}",
            }
            if server_port in (80, 443):
                allowed.update({"http://localhost", "http://127.0.0.1"})

            if origin not in allowed:
                self._send_json(
                    {"error": f"Forbidden: Cross-origin request from {origin} rejected"},
                    status=403,
                )
                return False
        return True

    def do_GET(self) -> None:  # noqa: N802
        if not self._validate_host():
            return

        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path.startswith("/api/requeue") or path == "/api/enqueue":
            self._send_json({"error": "Method Not Allowed"}, status=405)
            return

        if path in ("", "/"):
            self._send_html(get_dashboard_html())
            return

        q = JobQueue(self.db_path, read_only=True)
        try:
            if path == "/api/stats":
                counts = q.count_by_status()
                total = sum(counts.values())
                counts["total"] = total
                self._send_json(counts)
                return

            if path == "/api/jobs":
                query_params = urllib.parse.parse_qs(parsed.query)
                status_filter = query_params.get("status", [None])[0]
                queue_filter = query_params.get("queue", [None])[0]
                limit = int(query_params.get("limit", ["100"])[0])

                status_enum = JobStatus(status_filter) if status_filter else None
                jobs = q.list_jobs(status=status_enum, queue_name=queue_filter, limit=limit)
                self._send_json([j.to_dict() for j in jobs])
                return

            if path.startswith("/api/jobs/"):
                job_id = path[len("/api/jobs/"):]
                job = q.get(job_id)
                if job is None:
                    self._send_json({"error": f"Job {job_id} not found"}, status=404)
                    return
                attempts = q.get_attempts(job_id)
                self._send_json({
                    "job": job.to_dict(),
                    "attempts": [a.to_dict() for a in attempts],
                })
                return

            self._send_json({"error": "Not found"}, status=404)
        finally:
            q.close()

    def do_POST(self) -> None:  # noqa: N802
        if not self._validate_host():
            return

        if path.startswith("/api/requeue/") or path == "/api/enqueue":
            if not self._validate_csrf_and_origin():
                return

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b""

        q = JobQueue(self.db_path)
        try:
            if path.startswith("/api/requeue/"):
                job_id = path[len("/api/requeue/"):]
                try:
                    requeued = q.requeue(job_id)
                    self._send_json({"success": True, "job": requeued.to_dict()})
                except JobNotFoundError as err:
                    self._send_json({"error": str(err)}, status=404)
                except InvalidStateTransitionError as err:
                    self._send_json({"error": str(err)}, status=400)
                return

            if path == "/api/enqueue":
                try:
                    data = json.loads(body.decode("utf-8")) if body else {}
                    task_name = str(data.get("task_name", ""))
                    payload = data.get("payload", "{}")
                    max_attempts = int(data.get("max_attempts", 3))
                    queue_name = str(data.get("queue_name", "default"))
                    delay = float(data.get("delay", 0.0))

                    job = q.enqueue(
                        task_name=task_name,
                        payload=payload,
                        max_attempts=max_attempts,
                        queue_name=queue_name,
                        delay=delay,
                    )
                    self._send_json(
                        {"success": True, "job": job.to_dict()},
                        status=HTTPStatus.CREATED,
                    )
                except Exception as err:
                    self._send_json({"error": str(err)}, status=400)
                return

            self._send_json({"error": "Not found"}, status=404)
        finally:
            q.close()


def create_dashboard_server(
    db_path: str | Path,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> HTTPServer:
    """Create and configure the Dashboard HTTP server."""
    db_path_obj = Path(db_path).resolve()

    def handler_factory(*args: Any, **kwargs: Any) -> DashboardRequestHandler:
        return DashboardRequestHandler(*args, db_path=db_path_obj, **kwargs)

    return HTTPServer((host, port), handler_factory)


def run_dashboard(
    db_path: str | Path,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> None:
    """Run the local dashboard HTTP server indefinitely."""
    server = create_dashboard_server(db_path, host, port)
    logger.info("Serving jobqueue dashboard on http://%s:%d (db: %s)", host, port, db_path)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopping dashboard server")
    finally:
        server.server_close()
