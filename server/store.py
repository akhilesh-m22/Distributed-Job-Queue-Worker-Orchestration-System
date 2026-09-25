"""SQLite persistence layer: the single source of truth for job state.

The store is deliberately **the only place job state lives**. The in-memory
queue server keeps nothing durable besides the set of connected workers; every
claim / ack / fail / requeue is a transactional SQL update. That is what gives
us crash safety: restart the server and the same SQLite file still knows every
job's exact state, so running jobs can be recovered (re-scheduled) instead of
being forgotten.

Concurrency model
-----------------
SQLite connection objects are not thread-safe, and the queue server serves
many sockets concurrently. We therefore guard every operation with a single
per-``Store`` ``RLock``. In production you would scale past a single writer
(WAL mode + a busier lock discipline, or swap in Postgres), but a single
serialized writer is the *correct* choice here because every state transition
is already a tiny, fast UPDATE. WAL mode still lets readers never block the
writer and gives better crash robustness than the default rollback journal.

Timestamps
----------
Stored as naive-UTC strings in ``YYYY-MM-DD HH:MM:SS.ffffff`` form. Fixed
width means lexicographic ordering == chronological ordering, so SQLite can
compare ``scheduled_at <= ?`` directly on strings.
"""

import json
import random
import sqlite3
import threading
import uuid
from datetime import datetime, timezone

# Job lifecycle states.
STATUS_QUEUED = "queued"        # waiting for a worker to pick it up
STATUS_RUNNING = "running"      # checked out by a worker, not yet acked
STATUS_SUCCEEDED = "succeeded"  # worker acknowledged completion
STATUS_FAILED = "failed"        # worker reported failure; auto-retry scheduled
STATUS_DEAD_LETTER = "dead-letter"  # exhausted max_attempts; needs manual action


def utcnow():
    """Naive-UTC string timestamp, fixed width (microseconds)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def iso_to_display(ts):
    """Convert a stored naive-UTC timestamp to an ISO-8601 string with Z."""
    if ts is None:
        return None
    return ts.replace(" ", "T") + "Z"


def exponential_backoff(attempt, base=1.0, cap=30.0, jitter=0.25):
    """Delay for the retry that follows ``attempt`` failed tries.

    ``delay = min(cap, base * 2 ** (attempt - 1)) + uniform jitter``

    The exponential curve gives early retries for transient blips (DB lock,
    rate limit) while backing off hard on persistent failures so a wedged
    task can't hammer the system. Jitter de-synchronizes many jobs failing at
    the same instant (e.g. a downstream outage) so they don't all retry in
    lockstep — the classic thundering-herd problem.
    """
    delay = min(cap, base * (2 ** (attempt - 1)))
    delay += random.uniform(0, jitter * base)
    return round(delay, 3)


class Store:
    """Thread-safe SQLite backend for job persistence."""

    def __init__(self, db_path, max_attempts=5, backoff_base=1.0,
                 backoff_cap=30.0):
        self._lock = threading.RLock()
        self.db_path = db_path
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap

        # ``check_same_thread=False`` + our own lock: the single connection is
        # shared across all of the server's threads but only ever used while
        # holding ``self._lock``.
        self._conn = sqlite3.connect(
            db_path, isolation_level=None, check_same_thread=False
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_schema()

    # ------------------------------------------------------------------ #
    # Schema
    # ------------------------------------------------------------------ #
    def _create_schema(self):
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id            TEXT PRIMARY KEY,      -- uuid4 hex
                    task_type     TEXT NOT NULL,
                    payload       TEXT NOT NULL,          -- JSON
                    status        TEXT NOT NULL,
                    priority      INTEGER NOT NULL DEFAULT 0,
                    attempts      INTEGER NOT NULL DEFAULT 0,  -- times claimed
                    max_attempts  INTEGER NOT NULL DEFAULT 5,
                    created_at    TEXT NOT NULL,
                    updated_at    TEXT NOT NULL,
                    scheduled_at  TEXT NOT NULL,          -- may be claimed when now >= scheduled_at
                    started_at    TEXT,                   -- last time a worker claimed it
                    finished_at   TEXT,
                    last_error    TEXT,
                    worker_id     TEXT,                   -- worker currently holding it
                    result        TEXT                    -- success result (JSON)
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_claim
                    ON jobs(status, priority, created_at);
                CREATE INDEX IF NOT EXISTS idx_jobs_scheduled
                    ON jobs(status, scheduled_at);
                """
            )

    # ------------------------------------------------------------------ #
    # Row <-> dict helpers
    # ------------------------------------------------------------------ #
    _COLS = [
        "id", "task_type", "payload", "status", "priority", "attempts",
        "max_attempts", "created_at", "updated_at", "scheduled_at",
        "started_at", "finished_at", "last_error", "worker_id", "result",
    ]

    def _row_to_job(self, row):
        job = dict(zip(self._COLS, row))
        job["payload"] = json.loads(job["payload"])
        job["result"] = json.loads(job["result"]) if job["result"] else None
        return job

    def _fetchone(self, sql, params=()):
        with self._lock:
            row = self._conn.execute(sql, params).fetchone()
        return self._row_to_job(row) if row else None

    def _fetchall_where(self, where, params, limit):
        sql = "SELECT " + ", ".join(self._COLS) + " FROM jobs"
        if where:
            sql += " WHERE " + where
        sql += " ORDER BY created_at DESC"
        if limit:
            sql += " LIMIT ?"
            params = params + (limit,)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_job(r) for r in rows]

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def create_job(self, task_type, payload, priority=0, max_attempts=None):
        """Enqueue a new job. Returns the created job dict."""
        now = utcnow()
        job_id = uuid.uuid4().hex
        max_attempts = max_attempts or self.max_attempts
        with self._lock:
            self._conn.execute(
                "INSERT INTO jobs (id, task_type, payload, status, priority, "
                "attempts, max_attempts, created_at, updated_at, scheduled_at) "
                "VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
                (job_id, task_type, json.dumps(payload), STATUS_QUEUED,
                 int(priority), max_attempts, now, now, now),
            )
        return self.get_job(job_id)

    def get_job(self, job_id):
        return self._fetchone(
            "SELECT " + ", ".join(self._COLS) + " FROM jobs WHERE id = ?",
            (job_id,),
        )

    def claim_next(self, worker_capabilities, worker_id, now=None):
        """Atomically hand the best runnable job to a worker.

        Returns a job dict with ``attempts`` already incremented, or None.

        This is the core of **at-least-once delivery**: the job is marked
        ``running`` (invisible to other workers) but NOT deleted, so if the
        worker crashes before acking, a monitor can put it back in the queue.
        "Runnable" = status is ``queued`` AND backoff window has elapsed.
        Jobs are ordered by priority (higher = sooner) then FIFO by age.
        """
        now = now or utcnow()

        # Build the eligibility clause. A job is claimable if it is due
        # (backoff window elapsed) and its task type is supported by the
        # requesting worker. Both ``queued`` and ``failed`` jobs are eligible:
        # ``failed`` is how we represent "awaiting an automatic retry" (its
        # ``scheduled_at`` holds the exponential-backoff wake-up time, which
        # is exactly what this ``<=`` filter gates on).
        where = "status IN (?, ?) AND scheduled_at <= ?"
        params = [STATUS_QUEUED, STATUS_FAILED, now]
        if worker_capabilities:
            placeholders = ",".join("?" for _ in worker_capabilities)
            where += " AND task_type IN (" + placeholders + ")"
            params += list(worker_capabilities)

        with self._lock:
            row = self._conn.execute(
                "SELECT " + ", ".join(self._COLS) + " FROM jobs WHERE " + where
                + " ORDER BY priority DESC, created_at ASC LIMIT 1",
                params,
            ).fetchone()
            if row is None:
                return None
            job = self._row_to_job(row)
            self._conn.execute(
                "UPDATE jobs SET status = ?, attempts = attempts + 1, "
                "started_at = ?, worker_id = ?, updated_at = ? WHERE id = ?",
                (STATUS_RUNNING, now, worker_id, now, job["id"]),
            )
        # Reflect the transition in the dict handed to the caller.
        job["status"] = STATUS_RUNNING
        job["attempts"] += 1
        job["started_at"] = now
        job["worker_id"] = worker_id
        job["updated_at"] = now
        return job

    def ack_job(self, job_id, worker_id, result, now=None):
        """Mark a running job as succeeded (idempotent).

        Returns True if *this* worker's ack performed the transition, False if
        it was stale (e.g. the job was already requeued and run elsewhere) —
        which is exactly the duplicate-execution hazard of at-least-once
        delivery. The status flip happens exactly once because the WHERE
        clause only matches ``running`` jobs held by the same worker.
        """
        now = now or utcnow()
        with self._lock:
            row = self._conn.execute(
                "SELECT status, worker_id FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if not (row and row[0] == STATUS_RUNNING and row[1] == worker_id):
                return False
            self._conn.execute(
                "UPDATE jobs SET status = ?, result = ?, finished_at = ?, "
                "updated_at = ? WHERE id = ?",
                (STATUS_SUCCEEDED, json.dumps(result), now, now, job_id),
            )
            return True

    def fail_job(self, job_id, worker_id, error, now=None):
        """Record a worker-reported failure and schedule the next retry.

        Retry decision (the heart of the retry logic):

        * The job was ``claimed`` ``attempts`` times already.
        * If ``attempts >= max_attempts`` -> permanent failure: it is moved to
          the **dead-letter** state and will NOT run again unless an operator
          requeues it via the API.
        * Otherwise it becomes ``failed`` with ``scheduled_at`` set to
          ``now + exponential_backoff(attempts)``. Backoff is computed from
          the number of failed attempts so each subsequent retry waits longer.

        Only the worker that currently holds the job may fail it; a stale
        failure (job already recovered elsewhere) is a no-op.

        Returns a dict describing the outcome::

            {"retry": bool, "attempt": int, "retry_at": str|None,
             "dead_letter": bool}
        """
        now = now or utcnow()
        with self._lock:
            row = self._conn.execute(
                "SELECT status, worker_id, attempts FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            if not (row and row[0] == STATUS_RUNNING and row[1] == worker_id):
                return {"retry": False, "attempt": None, "retry_at": None,
                        "dead_letter": False}
            attempts = row[2]
            if attempts >= self.max_attempts:
                self._conn.execute(
                    "UPDATE jobs SET status = ?, last_error = ?, "
                    "finished_at = ?, updated_at = ? WHERE id = ?",
                    (STATUS_DEAD_LETTER, error, now, now, job_id),
                )
                return {"retry": False, "attempt": attempts,
                        "retry_at": None, "dead_letter": True}
            retry_at = self._schedule_retry_at(attempts, now)
            self._conn.execute(
                "UPDATE jobs SET status = ?, last_error = ?, scheduled_at = ?, "
                "finished_at = NULL, worker_id = NULL, updated_at = ? WHERE id = ?",
                (STATUS_FAILED, error, retry_at, now, job_id),
            )
            return {"retry": True, "attempt": attempts, "retry_at": retry_at,
                    "dead_letter": False}

    def _schedule_retry_at(self, attempts, now):
        """Compute the wall-clock time when attempt (attempts+1) may start."""
        delay = exponential_backoff(attempts, self.backoff_base,
                                    self.backoff_cap)
        return self._add_seconds(now, delay)

    @staticmethod
    def _add_seconds(ts, seconds):
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        return datetime.fromtimestamp(dt.timestamp() + seconds).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )

    def requeue_job(self, job_id, reason, now=None, reset_attempts=False):
        """Return a ``running`` job to the ``queued`` state without counting
        it as a task failure.

        Used by the liveness monitor when a worker dies mid-job and at server
        startup to recover jobs orphaned by a previous crash. We deliberately
        do NOT increment ``attempts``: crashing mid-execution is not the
        task's fault, and burning a task's retry budget on infrastructure
        failures hides real task errors.
        """
        now = now or utcnow()
        with self._lock:
            row = self._conn.execute(
                "SELECT status, attempts FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if not row or row[0] != STATUS_RUNNING:
                return False
            attempts = 0 if reset_attempts else row[1]
            self._conn.execute(
                "UPDATE jobs SET status = ?, scheduled_at = ?, worker_id = NULL, "
                "started_at = NULL, last_error = COALESCE(last_error, ?), "
                "attempts = ?, updated_at = ? WHERE id = ?",
                (STATUS_QUEUED, now, reason, attempts, now, job_id),
            )
            return True

    def retry_dead_letter(self, job_id, now=None):
        """Requeue a dead-lettered job with a fresh retry budget.

        The operator (or an automated policy) has decided the job is worth
        another shot — e.g. after fixing the downstream outage. We reset the
        attempt counter so it gets a full ``max_attempts`` again.
        """
        now = now or utcnow()
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if not row or row[0] != STATUS_DEAD_LETTER:
                return False
            self._conn.execute(
                "UPDATE jobs SET status = ?, attempts = 0, scheduled_at = ?, "
                "finished_at = NULL, last_error = NULL, worker_id = NULL, "
                "updated_at = ? WHERE id = ?",
                (STATUS_QUEUED, now, now, job_id),
            )
            return True

    def recover_unfinished(self, now=None):
        """Move any stale ``running`` jobs back to ``queued``.

        Called on server startup: if we crashed, the workers are gone and any
        job left ``running`` would otherwise be lost forever. This is the
        restart half of at-least-once delivery — the other half is the live
        heartbeat monitor. Returns the number of recovered jobs.
        """
        now = now or utcnow()
        with self._lock:
            cur = self._conn.execute(
                "UPDATE jobs SET status = ?, scheduled_at = ?, worker_id = NULL, "
                "started_at = NULL, last_error = 'recovered after server restart', "
                "updated_at = ? WHERE status = ?",
                (STATUS_QUEUED, now, now, STATUS_RUNNING),
            )
            return cur.rowcount

    # ------------------------------------------------------------------ #
    # Queries for the API
    # ------------------------------------------------------------------ #
    def list_jobs(self, status=None, task_type=None, limit=100):
        clauses = []
        params = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if task_type:
            clauses.append("task_type = ?")
            params.append(task_type)
        where = " AND ".join(clauses)
        return self._fetchall_where(where, tuple(params), limit)

    def list_dead_letter(self, limit=100):
        return self.list_jobs(status=STATUS_DEAD_LETTER, limit=limit)

    def counts(self):
        """Counts per status plus total executions, for dashboard/tests."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) FROM jobs GROUP BY status"
            ).fetchall()
            total_jobs = self._conn.execute(
                "SELECT COUNT(*) FROM jobs"
            ).fetchone()[0]
            total_executions = self._conn.execute(
                "SELECT COALESCE(SUM(attempts), 0) FROM jobs"
            ).fetchone()[0]
        counts = {s: 0 for s in (STATUS_QUEUED, STATUS_RUNNING, STATUS_SUCCEEDED,
                                 STATUS_FAILED, STATUS_DEAD_LETTER)}
        for status, n in rows:
            counts[status] = n
        counts["total"] = total_jobs
        counts["executions"] = total_executions
        return counts

    def close(self):
        with self._lock:
            self._conn.close()