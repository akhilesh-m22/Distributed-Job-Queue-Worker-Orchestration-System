# Five-minute interview walkthrough

## 0:00 — The problem

“I built the broker, durable state machine, and worker protocol to show what happens when a worker fails after taking responsibility for a job.”

Run `python scripts/demo.py`. The dashboard uses real broker data, with no fake metrics.

## 0:45 — Follow a job

Submit `generate_report` with `rows: 250` and `delay_secs: 5`. Inspect it while running. Explain that a claim records status and ownership; it does not remove the job. Inspect the result when it succeeds.

## 1:30 — Retries and dead letters

Run the demo scenario. Filter Retry scheduled to explain exponential backoff. Inspect the dead letter's error and exhausted budget. Manual retry resets the budget but does not repair a bad payload. Submit a clean replacement to show success after correcting the fault.

## 2:30 — Prove crash recovery

```bash
python scripts/load_test.py --jobs 100 --workers 3 --slow-jobs 2 --slow-delay 3
```

This creates a separate cluster and kills only a worker it started. Its jobs do not appear in the dashboard cluster. Show the zero-loss report and the victim job's multiple claims.

## 3:30 — Explain the hard parts

- Connection ownership prevents a client from impersonating another session.
- The broker enforces one in-flight job even when a buggy client fetches twice.
- Heartbeats do not prove task progress; deadlines and isolated task execution are future work.
- A partitioned worker can still perform side effects. Session fencing protects broker state; the destination must enforce idempotency too.
- SQLite makes a single broker explainable. Multi-broker HA requires a different coordination design.

## 4:30 — Engineering discipline

Show regression tests and CI: retry budgets, malformed frames, concurrent deduplication, runtime configuration, non-root containers, and health probes. Discuss an honest next step such as task timeouts, retention, or leader election.
