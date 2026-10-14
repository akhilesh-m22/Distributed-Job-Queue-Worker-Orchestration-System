"""Simulated email outbox; UNIQUE keys make retries safe across worker processes.

This records a delivery, not a real SMTP send. Real integrations must use their
provider's idempotency key or a transactional outbox dispatcher.
"""
import json
import os
import sqlite3
import time
from pathlib import Path

from worker.tasks.registry import register, injectable_failure


@register("send_email")
def send_email(payload, ctx):
    injectable_failure(payload, ctx)
    message_id = payload.get("message_id") or ctx["job_id"]
    recipient = payload.get("to", "person@example.com")
    time.sleep(payload.get("delay_secs", 0.05))
    directory = Path(os.getenv("DJQ_OUTPUT_DIR", "var"))
    directory.mkdir(parents=True, exist_ok=True)
    record = {"message_id": message_id, "to": recipient, "subject": payload.get("subject", ""),
              "job_id": ctx["job_id"], "worker": ctx["worker_id"],
              "attempt": ctx["attempt"], "sent_at": time.time()}
    conn = sqlite3.connect(directory / "outbox.db", timeout=30)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS deliveries (message_id TEXT PRIMARY KEY, record TEXT NOT NULL)")
        with conn:
            inserted = conn.execute("INSERT OR IGNORE INTO deliveries VALUES (?, ?)",
                                    (message_id, json.dumps(record))).rowcount
    finally:
        conn.close()
    return {"message_id": message_id, "to": recipient, "status": "sent", "duplicate": not inserted}
