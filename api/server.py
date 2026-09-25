"""Flask REST API layered on top of the queue server.

The API runs in the same process as the queue server so it can reach the
shared ``Store`` and the in-memory worker registry directly. In a production
setup you would split this into its own service, but that would not change
any of the guarantees — the store is the source of truth either way.

Endpoints
---------
POST /jobs                    -> submit a job
GET  /jobs                    -> list jobs (optional ?status=&task_type=&limit=)
GET  /jobs/<job_id>           -> job status & history
GET  /workers                 -> connected/dead workers with heartbeat status
GET  /dead-letter             -> dead-lettered jobs
POST /dead-letter/<job_id>/retry  -> requeue a dead-lettered job
GET  /stats                   -> counts per status
GET  /health                  -> liveness probe
"""

import threading

from flask import Flask, jsonify, request

from server import store as store_mod


def create_app(queue_server):
    app = Flask(__name__)
    store = queue_server.store

    def job_dict(job):
        """Shape a DB row for JSON responses."""
        return {
            "job_id": job["id"],
            "task_type": job["task_type"],
            "payload": job["payload"],
            "status": job["status"],
            "priority": job["priority"],
            "attempts": job["attempts"],
            "max_attempts": job["max_attempts"],
            "created_at": store_mod.iso_to_display(job["created_at"]),
            "updated_at": store_mod.iso_to_display(job["updated_at"]),
            "scheduled_at": store_mod.iso_to_display(job["scheduled_at"]),
            "started_at": store_mod.iso_to_display(job["started_at"]),
            "finished_at": store_mod.iso_to_display(job["finished_at"]),
            "last_error": job["last_error"],
            "worker_id": job["worker_id"],
            "result": job["result"],
        }

    @app.get("/health")
    def health():
        return jsonify({"status": "ok", "server": "djqs"})

    @app.post("/jobs")
    def submit_job():
        body = request.get_json(silent=True) or {}
        task_type = body.get("task_type")
        if not task_type or not isinstance(task_type, str):
            return jsonify({"error": "task_type is required"}), 400
        payload = body.get("payload")
        if payload is None:
            payload = {}
        priority = int(body.get("priority", 0) or 0)
        try:
            job = store.create_job(
                task_type, payload, priority,
                max_attempts=body.get("max_attempts"),
            )
        except TypeError as exc:
            return jsonify({"error": "invalid max_attempts: %s" % exc}), 400
        return jsonify(
            {"job_id": job["id"], "status": job["status"], "task_type": task_type}
        ), 202

    @app.get("/jobs")
    def list_jobs():
        status = request.args.get("status")
        task_type = request.args.get("task_type")
        try:
            limit = int(request.args.get("limit", 100))
        except ValueError:
            return jsonify({"error": "limit must be an integer"}), 400
        jobs = store.list_jobs(status=status, task_type=task_type, limit=limit)
        return jsonify({"jobs": [job_dict(j) for j in jobs],
                        "count": len(jobs)})

    @app.get("/jobs/<job_id>")
    def get_job(job_id):
        job = store.get_job(job_id)
        if job is None:
            return jsonify({"error": "job not found"}), 404
        return jsonify(job_dict(job))

    @app.get("/workers")
    def list_workers():
        return jsonify({"workers": queue_server.worker_snapshots(),
                        "count": len(queue_server.worker_snapshots())})

    @app.get("/dead-letter")
    def dead_letter():
        jobs = store.list_dead_letter()
        return jsonify({"jobs": [job_dict(j) for j in jobs],
                        "count": len(jobs)})

    @app.post("/dead-letter/<job_id>/retry")
    def retry_dead_letter(job_id):
        ok = store.retry_dead_letter(job_id)
        if not ok:
            return jsonify({"error": "job not in dead-letter state"}), 404
        return jsonify({"job_id": job_id, "status": "queued",
                        "message": "requeued for retry"})

    @app.get("/stats")
    def stats():
        counts = store.counts()
        return jsonify({**counts,
                        "workers": queue_server.worker_snapshots(),
                        "connected_workers": len(queue_server.connected_workers)})

    return app


def run_api(queue_server, host="127.0.0.1", port=5010, threads=32):
    """Run the REST API with a production WSGI server in a daemon thread.

    Flask's built-in werkzeug dev server is the wrong tool here: on Windows it
    intermittently stalls under sudden bursts of concurrent connections, which
    is precisely what the load test needs to survive. Waitress is a mature,
    threads-based WSGI server with a proper fixed thread pool and predictable
    behaviour under load ("do not use the Flask dev server in production" is
    printed by werkzeug itself for good reason).
    """
    app = create_app(queue_server)
    try:
        from waitress import serve
    except ImportError:  # pragma: no cover - waitress ships in requirements
        thread = threading_helper_start(app, host, port)
        return app, thread
    thread = threading.Thread(
        target=serve,
        kwargs={"app": app, "host": host, "port": port, "threads": threads,
                "channel_timeout": 120, "cleanup_interval": 5},
        name="djqs-api", daemon=True,
    )
    thread.start()
    return app, thread


def threading_helper_start(app, host, port):
    """Fallback: run Flask's dev server in a background daemon thread."""
    thread = threading.Thread(
        target=lambda: app.run(host=host, port=port, threaded=True,
                               use_reloader=False),
        name="djqs-api", daemon=True,
    )
    thread.start()
    return thread