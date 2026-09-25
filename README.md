# Distributed Job Queue & Worker Orchestration System

A lightweight, language-agnostic distributed job queue with durable scheduling,
at-least-once delivery, retries with exponential backoff, a dead-letter queue,
heartbeat-based worker recovery, and a REST control plane. Built as if it were a
minimal Celery/Sidekiq from first principles — no pub/sub broker, no external
queuing dependency; the broker, the worker protocol, and the persistence layer
are all implemented here.

```
                     ┌──────────────────────────────────────────────┐
                     │                QUEUE SERVER                  │
   submit jobs ────►  │                                              │
  (REST :5010)       │  ┌─────────────┐   ┌──────────┐   ┌────────┐  │
                     │  │ REST API    │   │ TCP broker│──►│ SQLite │  │
                     │  │ (waitress)  │   │ (port    │   │ store  │  │
                     │  │             │   │  5555)   │   │ (WAL)  │  │
                     │  └─────────────┘   │          │   └────────┘  │
                     │                    │  ◄──register/fetch────── │
                     │                    └──┼──heartbeat/ack/fail──►│
                     └──────────────────────┼───────────────────────┘
                                            │
                              ┌─────────────┼──────────────┐
                              ▼             ▼              ▼
                         ┌─────────┐   ┌─────────┐   ┌─────────┐
                         │ worker 0│   │ worker 1│   │ worker 2│
                         └─────────┘   └─────────┘   └─────────┘
                             one synchronous job at a time each
```

## What it does

- **Broker + workers over raw TCP** with a tiny length-prefixed JSON protocol.
- **Durable scheduling**: jobs live in SQLite (WAL mode), claimed jobs are
  marked `running`, not deleted.
- **At-least-once delivery**: a job is only considered done when the worker
  sends `ack`. Any failure to ack (crash, killed process, dropped socket,
  missed heartbeat) causes the job to be requeued and run again elsewhere.
- **Retries with exponential backoff + jitter**, then a **dead-letter queue**
  after `max_attempts`; dead-lettered jobs can be inspected and manually retried
  through the API.
- **Heartbeat monitoring**: the server requeues jobs whose worker goes silent
  (killed, hung, or network partition). Three independent recovery paths cover
  every failure mode (see [Failure recovery](#failure-recovery)).
- **REST API** for submitting jobs, inspecting the queue, live workers, dead
  letters, and stats.
- **Task registry with capability matching**: workers advertise what task types
  they can run; a job is only dispatched to a worker that advertises the type.
- **Priorities**: integer priority, lower value runs first.

## Quickstart

```bash
pip install -r requirements.txt

# 1. Start one queue server (TCP broker + REST API on :5010)
python -m server --port 5555 --api-port 5010 --db var/queue.db

# 2. Start as many workers as you like
python -m worker --name w1 --port 5555
python -m worker --name w2 --port 5555

# 3. Submit a job
curl -X POST http://127.0.0.1:5010/jobs -H "Content-Type: application/json" \
     -d '{"task_type": "send_email", "payload": {"to": "a@b.c", "subject": "hi"}}'
# => {"job_id": "..."}

# 4. Watch it move through the system
curl http://127.0.0.1:5010/jobs          # status, attempts, timestamps
curl http://127.0.0.1:5010/workers       # live workers + current job
curl http://127.0.0.1:5010/stats
```

### Kill a worker mid-job and watch the queue recover it

```bash
# In a terminal, watch statuses
curl http://127.0.0.1:5010/jobs

# Submit a slow job, then kill the worker running it (Ctrl+C / task manager).
# The job is requeued instantly (socket EOF) and a live worker completes it.
```

### Run the load test

`scripts/load_test.py` spins up a full cluster, pins a worker with a slow job,
**hard-kills it mid-processing**, floods the system with concurrent submissions,
and asserts zero job loss:

```bash
python scripts/load_test.py --jobs 100 --workers 3 --slow-jobs 3 --slow-delay 10
```

## Delivery guarantees & the exactly-once trap

Two kinds of "at least once" must be distinguished:

| Layer | Guarantee | How |
|---|---|---|
| *Completion* | **Exactly once** | `ack` is the only thing that marks a job `succeeded`; a job is dispatched to at most one worker at a time (strict one-job-per-worker protocol) |
| *Execution* | **At least once** | a crash between claiming and acking leaves the job `running`, so recovery requeues it and another worker runs it again |

Therefore **tasks must be idempotent** — that is the deal Celery/SQS-style
systems make. The bundled `send_email` task demonstrates idempotency by keying
writes on a `message_id` payload field so a duplicate execution is harmless.
This is why the failure-recovery paths below **requeue without incrementing
`attempts`**: an infrastructure failure is not the task's fault, so it should
not count against the retry budget.

## Failure recovery

A worker can vanish in three ways, and each has a dedicated recovery path:

1. **Socket EOF (graceful or abrupt close).** A per-connection handler thread
   sees the disconnect, and if the worker had an in-flight job it is requeued
   immediately.
2. **Process kill / network partition (no socket close).** Workers send a
   heartbeat every `--heartbeat-interval` seconds. The server's monitor thread
   scans the registry; any `connected` worker that hasn't sent a heartbeat in
   `--heartbeat-timeout` seconds is declared dead and its in-flight job is
   requeued.
3. **Server restart with a crashed worker.** On startup the server calls
   `recover_unfinished()`: any `running` job whose worker isn't connected is
   requeued.

Two extra details that make recovery safe:

- **Fresh `worker_id` per session.** After reconnecting, a worker registers with
  a *new* id. If it re-used the old one, its own heartbeats would keep the
  broker convinced the (dead) worker holding an orphaned in-flight job is still
  alive, so the monitor would never requeue it.
- **Idempotent acks/fails.** `ack`/`fail` are `UPDATE ... WHERE worker_id =
  current_worker AND status = 'running'`, so a stale or duplicate message from a
  zombie worker has no effect.

## Retries, backoff, and the dead-letter queue

- A `fail` immediately schedules a retry with delay
  `min(backoff_cap, backoff_base * 2 ** (attempts - 1))` plus small random
  jitter, so retries spread instead of thundering-herding.
- After `max_attempts` (default 5) a job becomes `dead-letter` and is never
  auto-run again.
- `POST /dead-letter/<job_id>/retry` resets attempts to 0 and requeues it.
- `--backoff-base`, `--backoff-cap`, `--max-attempts` are server CLI flags.
- Note: a worker that *fails* (task raised) sends `fail`, so the server controls
  the retry delay. A worker that *dies* never sends anything — recovery requeues
  immediately (path 1/2/3 above), because the failure was infrastructure, not
  task logic.

## Wire protocol (TCP)

Frames are `4-byte big-endian length + UTF-8 JSON`. Client → server:

| Message | Fields |
|---|---|
| `register` | `name`, `capabilities` |
| `heartbeat` | `worker_id` |
| `fetch` | `worker_id` |
| `ack` | `worker_id`, `job_id` |
| `fail` | `worker_id`, `job_id`, `error` |
| `unregister` | `worker_id` |

Server → client: `registered`, `job` (job_id, task_type, payload, priority),
`no_job`, `ack_ok`, `fail_ok`, `error`. The worker is strictly synchronous
(`fetch → job → execute → ack/fail → await ack_ok/fail_ok → fetch`), which is
what makes double-dispatch impossible.

## REST API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Submit a job (`task_type`, `payload`, optional `priority`) |
| `GET` | `/jobs` | List jobs (`?status=`, `?limit=`, `?offset=`) |
| `GET` | `/jobs/<id>` | Job detail: status, attempts, worker, timestamps, error |
| `GET` | `/workers` | Live workers: name, capabilities, heartbeat age, current job |
| `GET` | `/dead-letter` | Dead-lettered jobs |
| `POST` | `/dead-letter/<id>/retry` | Requeue with attempts reset to 0 |
| `GET` | `/stats` | Counts by status + total executions |
| `GET` | `/health` | Liveness probe |

The API lives in the server process (waitress, daemon thread), so it reads the
in-memory worker registry and the shared SQLite store without extra IPC.

## Storage

SQLite in WAL mode with a single connection guarded by a lock (good enough for a
single broker, and it makes ordering trivially correct). Naive-UTC timestamps.
One `jobs` table: `job_id, task_type, payload, priority, status, attempts,
created_at, scheduled_at, started_at, finished_at, current_worker, error`.

## Tasks

Tasks are plain Python functions registered under a name:

```python
# worker/tasks/email.py
@registry.register("send_email")
def send_email(payload):
    ...
```

The registry supports **capability matching** (`--capabilities send_email`
on a worker restricts it), and a test-only `injectable_failure` wrapper that
simulates flaky tasks via `payload.fail_first_n` / `payload.fail_probability`.
Three examples ship: `send_email` (idempotent), `resize_image` (simulated),
`generate_report`.

## Project layout

```
common/protocol.py     wire protocol (framing + message constructors)
server/store.py        SQLite store: statuses, backoff, recovery, stats
server/queue_server.py TCP broker, worker registry, heartbeat monitor
server/main.py         python -m server (broker + monitor + REST API)
api/server.py          Flask app + waitress runner
worker/worker.py       python -m worker (session loop, heartbeats)
worker/tasks/          registry + example tasks
scripts/load_test.py   cluster kill/recover load test
tests/                 6 pytest modules, 38 tests
```

## Design trade-offs vs. the big tools

| | This system | Celery/RabbitMQ | SQS | Kafka |
|---|---|---|---|---|
| Guarantee | at-least-once; exactly-once *completion* via ack + idempotent tasks | at-least-once | at-least-once (SQS) | at-least-once (Kafka exactly-once possible at high cost) |
| Broker | in-process TCP broker | RabbitMQ broker | hosted | log-based broker |
| Visibility/timeout | heartbeat + socket EOF (no explicit lock TTL) | ack + visibility via broker | visibility timeout | consumer group rebalance |
| Ordering | by priority, not strict FIFO | queue order | best-effort | partition order |
| Ops | zero external deps | RabbitMQ to run | vendor-managed | ZooKeeper/KRaft to run |

The trade-off in a nutshell: **one simple process, no broker to run, an
explainable recovery model** — at the cost of HA, ordering, and massive scale.
The power of the design is that every guarantee is derived from a tiny set of
primitives: *claim-marks-running, heartbeat-for-liveness, ack-is-truth, and
idempotent tasks.*

## Tests

```bash
python -m pytest tests -q        # 38 tests incl. recovery, protocol, API, retries
```

Highlights: a lodash-style `RawWorker` helper that speaks raw TCP frames
(garbage, fragmented, empty, oversized) to abuse the framing layer; `wait_for`
polling; `SpawnedWorker` for real-process tests; parametrized recovery tests
(EOF, heartbeat timeout, server restart); and flaky-task retry tests.