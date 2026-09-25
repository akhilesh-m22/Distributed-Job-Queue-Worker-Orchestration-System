"""Example task: mock email delivery.

In production this would call an SMTP/API provider. Here it simulates the
SMTP round-trip with a ``delay_secs`` sleep and appends a delivery line to a
log file so a human can eyeball that work actually happened.

Idempotency note: the delivery log is keyed by ``payload.message_id`` and we
never write duplicate lines for the same message — exactly the defensive
stance you adopt under an at-least-once broker, where the same job may be
executed more than once after a worker crash.
"""

import json
import os
import time
import uuid

from worker.tasks.registry import register, injectable_failure

SENT_LOG = os.path.join(os.path.dirname(__file__), "..", "..", "var",
                        "sent_emails.jsonl")


def _seen(message_id):
    """Return True if a message id already appears in the delivery log."""
    if not os.path.exists(SENT_LOG):
        return False
    with open(SENT_LOG, "r", encoding="utf-8") as fh:
        for line in fh:
            try:
                if json.loads(line).get("message_id") == message_id:
                    return True
            except ValueError:
                continue
    return False


def _append_line(record):
    os.makedirs(os.path.dirname(SENT_LOG), exist_ok=True)
    with open(SENT_LOG, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")


@register("send_email")
def send_email(payload, ctx):
    """Simulated SMTP send. Idempotent by message_id."""
    injectable_failure(payload, ctx)
    message_id = payload.get("message_id") or uuid.uuid4().hex
    recipient = payload.get("to", "person@example.com")

    time.sleep(payload.get("delay_secs", 0.05))  # emulate SMTP latency

    if not _seen(message_id):
        _append_line({
            "message_id": message_id, "to": recipient,
            "subject": payload.get("subject", ""),
            "job_id": ctx["job_id"], "worker": ctx["worker_id"],
            "attempt": ctx["attempt"], "sent_at": time.time(),
        })
        duplicate = False
    else:
        duplicate = True  # at-least-once: this is a re-run of a sent message

    return {"message_id": message_id, "to": recipient, "status": "sent",
            "duplicate": duplicate}