# Operations

## Local demo

The launcher refuses occupied ports and writes logs under `var/demo-logs`. A managed service exit stops the rest. Data stays in `var/demo.db`. Windows cleanup terminates managed process trees; unfinished jobs recover on restart. Unix cleanup requests termination and escalates after eight seconds.

## Single-host deployment

1. Install Docker Engine with Compose.
2. Copy `.env.example` to `.env`. For remote access, set a long random `DJQ_API_KEY`.
3. Run `docker compose up --build --wait --scale worker=3`.
4. Put an HTTPS reverse proxy in front of `127.0.0.1:3000`. Preserve the original Host header for same-origin write checks.
5. Enter the API token in Connection settings. Never expose the TCP broker publicly.

Only one broker can own the database. Workers share an output volume; queue and output volumes are separate. Existing volumes must be writable by container UID 10001. Host bind mounts require suitable permissions.

## Probes and metrics

`/health` is HTTP liveness. `/ready` checks the broker/monitor and database connection. Zero workers does not make the broker unready: it can still accept durable jobs. Worker count is visible separately.

`/metrics` exposes `djqs_jobs{status="…"}` and `djqs_workers`. A scraper needs a bearer token when auth is enabled. Keep it on the private network. These are current-state gauges.

## Backups

Use SQLite's online backup API. Copying a live database file without its WAL can omit committed work.

```bash
python -c "import sqlite3; src=sqlite3.connect('var/demo.db'); dst=sqlite3.connect('var/demo-backup.db'); src.backup(dst); dst.close(); src.close()"
```

For Compose:

```bash
docker compose exec broker python -c "import sqlite3; src=sqlite3.connect('/app/var/queue.db'); dst=sqlite3.connect('/app/var/backup.db'); src.backup(dst); dst.close(); src.close()"
docker compose cp broker:/app/var/backup.db ./queue-backup.db
```

To restore, stop the cluster, preserve the old database and sidecars for rollback, and restore into a fresh directory or volume before starting one broker. Never mix an old WAL with a restored database. Back up worker outputs separately.

## Diagnosis

| Symptom | Inspect |
|---|---|
| Dashboard disconnected | Broker readiness, `DJQ_API_URL`, logs |
| HTTP 401 | Broker token and dashboard Connection settings |
| Jobs remain queued | Workers, capabilities, priority traffic |
| Retry waits | `scheduled_at` backoff deadline |
| Dead letters | Error and per-job budget; fix the fault before retry |
| Repeated recoveries | Network, crash logs, heartbeat settings |
| Healthy worker, stuck task | Task may be hung; no execution timeout yet |

There is no automated retention policy. Archive/rotate deliberately. This is a deployable single-broker project, not an HA service with a production SLO.
