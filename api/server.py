"""Validated REST control plane shared with the single durable broker."""

import hmac
import os
import sqlite3
import threading

from flask import Flask, Response, jsonify, request
from werkzeug.exceptions import HTTPException

from server import store as store_mod

STATUSES = {"queued", "running", "succeeded", "failed", "dead-letter"}


def create_app(queue_server, api_key=None):
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 256 * 1024
    store = queue_server.store
    api_key = os.getenv("DJQ_API_KEY", "") if api_key is None else api_key

    @app.before_request
    def authenticate():
        if api_key and request.path not in {"/health", "/ready"}:
            supplied = request.headers.get("Authorization", "")
            if not hmac.compare_digest(supplied.encode(), ("Bearer " + api_key).encode()):
                return jsonify(error="A valid bearer token is required"), 401

    @app.after_request
    def response_headers(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.errorhandler(HTTPException)
    def http_error(exc):
        return jsonify(error=exc.description), exc.code

    def job_dict(job):
        result = {**job, "job_id": job["id"]}
        del result["id"]
        for field in ("created_at", "updated_at", "scheduled_at", "started_at", "finished_at"):
            result[field] = store_mod.iso_to_display(job[field])
        return result

    def pagination():
        try:
            limit = int(request.args.get("limit", 25))
            offset = int(request.args.get("offset", 0))
        except ValueError:
            raise ValueError("limit and offset must be integers")
        if not 1 <= limit <= 200 or not 0 <= offset <= 1_000_000:
            raise ValueError("limit must be 1–200 and offset must be 0–1000000")
        return limit, offset

    @app.get("/health")
    def health():
        return jsonify(status="ok", server="djqs")

    @app.get("/ready")
    def ready():
        try:
            available = queue_server.is_running and store.ping()
        except sqlite3.Error:
            available = False
        return jsonify(status="ready" if available else "unavailable"), 200 if available else 503

    @app.post("/jobs")
    def submit_job():
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify(error="Request body must be a JSON object"), 400
        task_type = body.get("task_type")
        if not isinstance(task_type, str) or not task_type.strip() or len(task_type) > 100:
            return jsonify(error="task_type must be a nonempty string of at most 100 characters"), 400
        payload = body.get("payload", {})
        if not isinstance(payload, dict):
            return jsonify(error="payload must be a JSON object"), 400
        priority = body.get("priority", 0)
        if type(priority) is not int or not -1000 <= priority <= 1000:
            return jsonify(error="priority must be an integer between -1000 and 1000"), 400
        try:
            job = store.create_job(task_type.strip(), payload, priority,
                                   max_attempts=body.get("max_attempts"))
        except ValueError as exc:
            return jsonify(error=str(exc)), 400
        response = jsonify(job_id=job["id"], status=job["status"], task_type=job["task_type"])
        response.headers["Location"] = "/jobs/" + job["id"]
        return response, 202

    def job_page(status=None):
        try:
            limit, offset = pagination()
        except ValueError as exc:
            return jsonify(error=str(exc)), 400
        status = status or request.args.get("status")
        if status and status not in STATUSES:
            return jsonify(error="Unknown job status"), 400
        jobs = store.list_jobs(status=status, task_type=request.args.get("task_type"),
                               limit=limit + 1, offset=offset, search=request.args.get("search"))
        return jsonify(jobs=[job_dict(j) for j in jobs[:limit]], count=len(jobs[:limit]),
                       limit=limit, offset=offset, has_more=len(jobs) > limit)

    @app.get("/jobs")
    def list_jobs():
        return job_page()

    @app.get("/jobs/<job_id>")
    def get_job(job_id):
        job = store.get_job(job_id)
        if job is None:
            return jsonify(error="Job not found"), 404
        return jsonify(job_dict(job))

    @app.get("/workers")
    def list_workers():
        snapshots = queue_server.worker_snapshots()
        return jsonify(workers=snapshots, count=len(snapshots))

    @app.get("/dead-letter")
    def dead_letter():
        return job_page("dead-letter")

    @app.post("/dead-letter/<job_id>/retry")
    def retry_dead_letter(job_id):
        if not store.retry_dead_letter(job_id):
            return jsonify(error="Job not in dead-letter state"), 404
        return jsonify(job_id=job_id, status="queued", message="Requeued for retry")

    @app.get("/stats")
    def stats():
        workers = queue_server.worker_snapshots()
        return jsonify(**store.counts(), workers=workers,
                       connected_workers=sum(w["is_alive"] for w in workers))

    @app.get("/metrics")
    def metrics():
        counts = store.counts()
        lines = ["# HELP djqs_jobs Jobs currently in each state.", "# TYPE djqs_jobs gauge"]
        for status in sorted(STATUSES):
            lines.append('djqs_jobs{status="%s"} %s' % (status, counts[status]))
        lines += ["# HELP djqs_workers Connected workers.", "# TYPE djqs_workers gauge",
                  "djqs_workers %s" % len(queue_server.connected_workers)]
        return Response("\n".join(lines) + "\n", content_type="text/plain; version=0.0.4")

    return app


def run_api(queue_server, host="127.0.0.1", port=5010, threads=16):
    """Bind synchronously so startup fails immediately if the port is occupied."""
    from waitress import create_server
    app = create_app(queue_server)
    http_server = create_server(app, host=host, port=port, threads=threads,
                                channel_timeout=30, max_request_body_size=256 * 1024)
    thread = threading.Thread(target=http_server.run, name="djqs-api", daemon=True)
    thread.http_server = http_server
    thread.start()
    return app, thread
