"""Example task: mock report generation.

Aggregates a batch of fake rows and writes a JSON report to ``var/reports/``.
Demonstrates a task that produces a durable artifact keyed by job_id — which
makes re-execution after a worker crash safe to reason about (the second run
overwrites the same file, so "exactly once" output is achievable even though
execution is at-least-once).
"""

import json
import os
import random
import time

from worker.tasks.registry import register, injectable_failure

REPORT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "var",
                          "reports")


@register("generate_report")
def generate_report(payload, ctx):
    """Build a small summary report over ``payload.rows`` fake sales records."""
    injectable_failure(payload, ctx)
    report_id = payload.get("report_id") or "job-%s" % ctx["job_id"]
    rows = int(payload.get("rows", 50))
    seed = payload.get("seed", ctx["job_id"])
    rng = random.Random(seed)

    # Simulate reading a data warehouse: generating rows is the "slow part".
    time.sleep(payload.get("delay_secs", 0.1))
    records = [{"region": rng.choice(["NA", "EU", "APAC"]),
                "amount": round(rng.uniform(10, 500), 2)}
               for _ in range(rows)]

    revenue = sum(r["amount"] for r in records)
    by_region = {}
    for r in records:
        by_region[r["region"]] = by_region.get(r["region"], 0.0) + r["amount"]

    report = {
        "report_id": report_id, "job_id": ctx["job_id"],
        "rows": rows, "total_revenue": round(revenue, 2),
        "by_region": {k: round(v, 2) for k, v in by_region.items()},
        "attempt": ctx["attempt"], "generated_at": time.time(),
    }

    os.makedirs(REPORT_DIR, exist_ok=True)
    path = os.path.join(REPORT_DIR, "%s.json" % report_id)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)

    return {**report, "path": path}