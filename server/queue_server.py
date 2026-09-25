"""TCP queue server: the broker at the centre of the system.

Responsibilities
----------------
1. Accept worker connections over TCP and route messages (register / heartbeat
   / fetch / ack / fail / unregister).
2. Dispatch jobs from SQLite to workers (one in-flight job per worker).
3. Monitor worker heartbeats and **requeue** jobs from workers that stop
   responding — this is the automatic-failure-recovery half of at-least-once
   delivery.
4. On startup, **recover** jobs left in ``running`` state by a previous
   crash/restart of this very process.

Design notes (interview-ready):
-------------------------------
* One thread per connection: the protocol is blocking and simple, and the
  workload is thousands, not millions, of connections. Back-pressure for the
  database is provided by the ``Store`` lock rather than an event loop.
* **Claim vs. ack**: a job is only removed from the queue when the worker
  explicitly acknowledges it. Between claim and ack the job sits in
  ``running`` state and is recoverable. If a worker dies, the monitor puts it
  back. If *this server* dies, ``recover_unfinished()`` puts it back on boot.
* Heartbeats are what make the monitor trustworthy: a TCP socket can stay
  "open" long after a peer process is gone (a killed machine, a wedged
  process), so socket closure alone is not a liveness signal. The periodic
  heartbeat thread on the worker is the real signal.
"""

import logging
import socket
import threading
import time
import uuid

from common import protocol

log = logging.getLogger("djqs.server")


class WorkerState:
    """In-memory record of a connected worker (ephemeral, not persisted)."""

    def __init__(self, worker_id, name, pid, capabilities, addr):
        self.id = worker_id
        self.name = name
        self.pid = pid
        self.capabilities = capabilities
        self.addr = addr
        self.connected_at = time.time()
        self.last_heartbeat = time.time()
        self.status = "connected"          # "connected" | "dead"
        self.dead_at = None
        # Current in-flight job (one per worker keeps requeue reasoning simple).
        self.current_job = None            # job_id or None
        self.current_job_type = None
        self.job_started_at = None

    @property
    def heartbeat_age(self):
        return round(time.time() - self.last_heartbeat, 3)

    def snapshot(self):
        """Public view returned by GET /workers."""
        return {
            "worker_id": self.id,
            "name": self.name,
            "pid": self.pid,
            "capabilities": self.capabilities,
            "address": "%s:%s" % self.addr[:2],
            "status": self.status,
            "current_job": self.current_job,
            "current_job_type": self.current_job_type,
            "job_started_at": self.job_started_at,
            "connected_at": self.connected_at,
            "last_heartbeat": self.last_heartbeat,
            "heartbeat_age_secs": self.heartbeat_age,
            "is_alive": self.status == "connected"
                        and self.heartbeat_age < self.heartbeat_timeout,
        }

    heartbeat_timeout = 4.0  # set per-instance by QueueServer at register


class QueueServer:
    """Blocking TCP broker with a heartbeat monitor thread."""

    def __init__(self, store, host="127.0.0.1", port=5555,
                 heartbeat_timeout=4.0, monitor_interval=0.5,
                 worker_prune_ttl=120.0):
        self.store = store
        self.host = host
        self.port = port
        self.heartbeat_timeout = heartbeat_timeout
        self.monitor_interval = monitor_interval
        self.worker_prune_ttl = worker_prune_ttl

        self._lock = threading.RLock()
        self._workers = {}       # worker_id -> WorkerState
        self._socks = {}         # worker_id -> socket (for future broadcast)
        self._running = False
        self._listener = None
        self._accept_thread = None
        self._monitor_thread = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    def start(self, background=True):
        """Bind, listen, start the monitor. Recovers orphaned jobs first."""
        recovered = self.store.recover_unfinished()
        if recovered:
            log.warning("recovered %d job(s) left running by a previous run",
                        recovered)

        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((self.host, self.port))
        self.port = self._listener.getsockname()[1]   # resolves port 0
        self._listener.listen(128)
        self._running = True

        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, name="heartbeat-monitor", daemon=True
        )
        self._monitor_thread.start()

        if background:
            self._accept_thread = threading.Thread(
                target=self._accept_loop, name="accept", daemon=True
            )
            self._accept_thread.start()
        log.info("queue server listening on %s:%s (heartbeat timeout %.1fs)",
                 self.host, self.port, self.heartbeat_timeout)
        return self

    def serve_forever(self):
        self._accept_loop()

    def stop(self):
        self._running = False
        with self._lock:
            socks = list(self._socks.values())
            self._socks.clear()
            self._workers.clear()
        for s in socks:
            try:
                s.close()
            except OSError:
                pass
        if self._listener:
            try:
                self._listener.close()
            except OSError:
                pass
        for t in (self._accept_thread, self._monitor_thread):
            if t and t.is_alive():
                t.join(timeout=2.0)
        log.info("queue server stopped")

    # ------------------------------------------------------------------ #
    # Accept loop & connection handling
    # ------------------------------------------------------------------ #
    def _accept_loop(self):
        while self._running:
            try:
                conn, addr = self._listener.accept()
            except OSError:
                break
            t = threading.Thread(
                target=self._handle_client, args=(conn, addr),
                name="client-%s:%s" % addr[:2], daemon=True,
            )
            t.start()

    def _handle_client(self, conn, addr):
        """Read frames until EOF; a crash here triggers recovery of the job."""
        worker_id = None
        try:
            while self._running:
                try:
                    msg = protocol.recv_message(conn)
                except (ConnectionResetError, protocol.ProtocolError, OSError):
                    break
                try:
                    wid = self._dispatch(conn, msg, addr)
                    if wid:
                        worker_id = wid
                except OSError:
                    # The peer vanished mid-reply (e.g. we were about to send
                    # an error to a socket that just closed). Treat as EOF.
                    break
        finally:
            # Socket died without an unregister (crash, network loss, kill).
            # That is *exactly* the moment at-least-once delivery must cover:
            # put the worker's in-flight job back on the queue so some other
            # worker can finish it. If the socket is merely wedged (no EOF)
            # the heartbeat monitor handles it instead. _fail_worker is a
            # no-op if the worker was already removed (graceful unregister).
            if worker_id:
                self._fail_worker(worker_id, reason="connection closed")
            try:
                conn.close()
            except OSError:
                pass

    def _dispatch(self, conn, msg, addr):
        """Route a single message. Returns the worker_id referenced by the
        message (or the fresh id on ``register``) so the client loop can track
        which worker owns this connection for crash-recovery purposes."""
        mtype = msg.get("type")
        if mtype == "register":
            worker_id = self._register(msg, conn, addr)
            protocol.send(conn, protocol.registered(worker_id))
            return worker_id
        worker_id = msg.get("worker_id")
        if mtype == "heartbeat":
            self._heartbeat(worker_id)
            # no reply: heartbeats are fire-and-forget
        elif mtype == "fetch":
            self._handle_fetch(conn, worker_id)
        elif mtype == "ack":
            self._handle_ack(conn, msg)
        elif mtype == "fail":
            self._handle_fail(conn, msg)
        elif mtype == "unregister":
            self._cleanup_worker(worker_id, "unregister")
        else:
            protocol.send(conn, protocol.server_error("unknown message type"))
        return worker_id

    # ------------------------------------------------------------------ #
    # Message handlers
    # ------------------------------------------------------------------ #
    def _register(self, msg, conn, addr):
        worker_id = uuid.uuid4().hex
        caps = list(set(msg.get("capabilities") or []))
        with self._lock:
            state = WorkerState(worker_id, msg.get("name", "worker"),
                                msg.get("pid"), caps, addr)
            state.heartbeat_timeout = self.heartbeat_timeout
            self._workers[worker_id] = state
            self._socks[worker_id] = conn
        log.info("worker registered: %s name=%s caps=%s",
                 worker_id[:8], state.name, caps)
        return worker_id

    def _heartbeat(self, worker_id):
        with self._lock:
            state = self._workers.get(worker_id)
            if state is None:
                return
            state.last_heartbeat = time.time()
            if state.status == "dead":
                # A worker we gave up on came back (transient network blip).
                # It may still be running the job we already requeued, so we
                # deliberately do NOT re-attach that job; when it acks, the
                # store rejects the stale ack because job.worker_id changed.
                log.info("worker %s revived after being marked dead",
                         worker_id[:8])
                state.status = "connected"

    def _handle_fetch(self, conn, worker_id):
        with self._lock:
            state = self._workers.get(worker_id)
        if state is None:
            protocol.send(conn, protocol.server_error("unknown worker"))
            return
        job = self.store.claim_next(state.capabilities, worker_id)
        if job is None:
            protocol.send(conn, protocol.no_job())
            return
        with self._lock:
            state.current_job = job["id"]
            state.current_job_type = job["task_type"]
            state.job_started_at = time.time()
        log.debug("granted job %s (%s) to worker %s attempt=%s",
                  job["id"][:8], job["task_type"], worker_id[:8], job["attempts"])
        protocol.send(conn, protocol.grant_job(
            job["id"], job["task_type"], job["payload"], job["attempts"]
        ))

    def _handle_ack(self, conn, msg):
        """Job completed successfully -> final removal from the queue."""
        job_id = msg.get("job_id")
        ok = self.store.ack_job(job_id, msg.get("worker_id"),
                                msg.get("result"))
        with self._lock:
            state = self._workers.get(msg.get("worker_id"))
            if state and state.current_job == job_id:
                state.current_job = None
                state.current_job_type = None
                state.job_started_at = None
        protocol.send(conn, {"type": "ack_ok", "job_id": job_id, "applied": ok})
        if not ok:
            # Stale ack: job was already requeued & finished elsewhere, or
            # never belonged to this worker. Expected under at-least-once.
            log.debug("ignored stale ack for job %s from worker %s",
                      (job_id or "?")[:8], (msg.get("worker_id") or "?")[:8])

    def _handle_fail(self, conn, msg):
        """Job failed -> decide: auto-retry with backoff, or dead-letter."""
        job_id = msg.get("job_id")
        outcome = self.store.fail_job(job_id, msg.get("worker_id"),
                                      msg.get("error", "unknown error"))
        with self._lock:
            state = self._workers.get(msg.get("worker_id"))
            if state and state.current_job == job_id:
                state.current_job = None
                state.current_job_type = None
                state.job_started_at = None
        if outcome.get("dead_letter"):
            log.warning("job %s dead-lettered after %s attempts",
                        (job_id or "?")[:8], outcome.get("attempt"))
        elif outcome.get("retry"):
            log.info("job %s failed (attempt %s/%s), retry at %s",
                     (job_id or "?")[:8], outcome.get("attempt"),
                     self.store.max_attempts, outcome.get("retry_at"))
        protocol.send(conn, {"type": "fail_ok", "job_id": job_id,
                             **outcome})

    def _cleanup_worker(self, worker_id, reason):
        """Graceful removal: requeue whatever it was holding, drop state."""
        self._fail_worker(worker_id, reason=reason)
        with self._lock:
            self._workers.pop(worker_id, None)
            sock = self._socks.pop(worker_id, None)
        if sock:
            try:
                sock.close()
            except OSError:
                pass
        return True

    # ------------------------------------------------------------------ #
    # Failure recovery (the requeue path)
    # ------------------------------------------------------------------ #
    def _fail_worker(self, worker_id, reason):
        """Mark a worker dead and requeue its in-flight job.

        This is the single place where a job that was 'checked out' but never
        acknowledged returns to the queue. It is idempotent: calling it twice
        (e.g. EOF handler + monitor) finds ``current_job`` already cleared and
        does nothing. ``requeue_job`` itself only flips ``running`` jobs back
        to ``queued``, so a job that has meanwhile succeeded is left alone.
        """
        job_id = None
        with self._lock:
            state = self._workers.get(worker_id)
            if state is None:
                return
            if state.status == "dead" and state.current_job is None:
                return
            state.status = "dead"
            state.dead_at = time.time()
            job_id, state.current_job = state.current_job, None
            state.current_job_type = None
            state.job_started_at = None

        if job_id:
            applied = self.store.requeue_job(
                job_id, reason="worker %s %s" % (worker_id[:8], reason)
            )
            log.warning(
                "requeued job %s from worker %s (%s) applied=%s",
                job_id[:8], worker_id[:8], reason, applied,
            )
        else:
            log.info("worker %s marked dead (%s); no in-flight job",
                     worker_id[:8], reason)

    # ------------------------------------------------------------------ #
    # Heartbeat monitor
    # ------------------------------------------------------------------ #
    def _monitor_loop(self):
        """Requeue jobs from workers that stop heartbeating.

        Why heartbeats and not just 'socket is open'? Because a socket stays
        open when the peer machine dies, when the OS kills the process and
        the kernel hasn't FIN'd yet, or when the process is wedged in an
        infinite loop. The worker runs a tiny daemon thread that sends a
        heartbeat every second; if we see no heartbeat within
        ``heartbeat_timeout`` we assume the worker is gone even if the socket
        looks healthy, and we give its job back to the pool.

        Trade-off (state it in an interview): this is *at-least-once*, not
        exactly-once. A network partition can make us requeue a job that the
        worker is still running, so the job may execute twice. Consumers must
        be idempotent (e.g. writes keyed by job_id) — the same contract
        Celery/SQS make you sign.
        """
        while self._running:
            time.sleep(self.monitor_interval)
            now = time.time()
            with self._lock:
                workers = list(self._workers.values())
            for state in workers:
                if state.status == "connected" and \
                        (now - state.last_heartbeat) > self.heartbeat_timeout:
                    log.warning(
                        "worker %s missed heartbeat (last %.1fs ago); "
                        "requeueing its job",
                        state.id[:8], now - state.last_heartbeat,
                    )
                    self._fail_worker(state.id, reason="heartbeat timeout")
                # Prune old dead workers so the API doesn't accumulate them.
                if state.status == "dead" and state.dead_at and \
                        (now - state.dead_at) > self.worker_prune_ttl:
                    with self._lock:
                        self._workers.pop(state.id, None)

    # ------------------------------------------------------------------ #
    # Introspection for the API
    # ------------------------------------------------------------------ #
    def worker_snapshots(self):
        with self._lock:
            return [w.snapshot() for w in self._workers.values()]

    @property
    def connected_workers(self):
        with self._lock:
            return [w.id for w in self._workers.values()
                    if w.status == "connected"]