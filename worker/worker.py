"""The worker: pulls jobs off the queue server and executes them.

Loop (per session)
------------------
1. Connect to the TCP broker and REGISTER our capabilities.
2. Start a daemon heartbeat thread (liveness signal the broker trusts).
3. FETCH -> get a job or ``no_job``; execute the task; report ACK or FAIL.
4. If the socket dies mid-job we *cannot* ack — the broker requeues the job
   via the heartbeat monitor, which is exactly the at-least-once pathway.

Why a *fresh* worker_id per session?
-------------------------------------
After a disconnect we reconnect and register anew. If we re-used the same id,
the broker would think the "same" worker is alive and keep the orphaned
in-flight job attached to us forever: our own heartbeats would prevent the
recovery monitor from ever requeueing it. A new id detaches us cleanly from
the old session's in-flight job and lets the monitor recover it independently.
"""

import argparse
import json
import logging
import os
import signal
import socket
import sys
import threading
import time

from common import protocol
from worker.tasks import registry
# Importing the three example task modules populates the registry.
from worker.tasks import email, images, reports  # noqa: F401

log = logging.getLogger("djqs.worker")


class Worker:
    def __init__(self, host, port, name="worker", capabilities=None,
                 heartbeat_interval=1.0, server_heartbeat_timeout=4.0):
        self.host = host
        self.port = port
        self.name = name
        self.capabilities = list(capabilities or registry.all_types())
        self.heartbeat_interval = heartbeat_interval
        # If our socket dies mid-job, wait this long for the server to notice
        # we are gone before it requeues our job. Mirrors the server timeout.
        self.server_heartbeat_timeout = server_heartbeat_timeout
        self._stop = threading.Event()
        self._send_lock = threading.Lock()
        self._sock = None
        self._worker_id = None

    # ------------------------------------------------------------------ #
    # Sending
    # ------------------------------------------------------------------ #
    def _send(self, msg):
        protocol.send(self._sock, msg, self._send_lock)

    # ------------------------------------------------------------------ #
    # Heartbeats
    # ------------------------------------------------------------------ #
    def _heartbeat_loop(self):
        """Daemon thread: keep the broker convinced we are alive."""
        while not self._stop.is_set() and self._sock:
            try:
                if self._worker_id:
                    self._send(protocol.heartbeat(self._worker_id))
            except (OSError, ValueError):
                return  # socket gone; session loop will reconnect
            self._stop.wait(self.heartbeat_interval)

    # ------------------------------------------------------------------ #
    # The run loop
    # ------------------------------------------------------------------ #
    def run_forever(self):
        # Reconnect forever: the server may restart, reboot, or drop us at any
        # moment and the worker is expected to ride through it.
        attempt = 0
        while not self._stop.is_set():
            try:
                self._run_session()
                attempt = 0
            except (OSError, ConnectionError, protocol.ProtocolError) as exc:
                log.warning("session ended (%s); reconnecting...", exc)
                attempt += 1
                self._stop.wait(min(10, 0.5 * attempt))  # small reconnect backoff
        log.info("worker stopped")

    def _run_session(self):
        """One connect -> register -> fetch/execute/ack cycle. Blocks."""
        sock = socket.create_connection((self.host, self.port), timeout=10)
        sock.settimeout(None)
        self._sock = sock
        # Register: get a brand new identity for this session (see module doc).
        self._send(protocol.register(self.name, os.getpid(), self.capabilities))
        msg = protocol.recv_message(sock)
        self._worker_id = msg["worker_id"]
        log.info("registered as %s (capabilities=%s)",
                 self._worker_id[:8], self.capabilities)

        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="heartbeat", daemon=True
        )
        heartbeat_thread.start()

        try:
            self._session_loop()
        finally:
            # Try to release our in-flight job gracefully so the server does
            # not have to wait for a heartbeat timeout.
            try:
                if self._worker_id:
                    self._send(protocol.unregister(self._worker_id))
            except OSError:
                pass
            self._sock = None
            self._worker_id = None
            try:
                sock.close()
            except OSError:
                pass

    def _session_loop(self):
        """Strict request/response session: fetch -> job -> ack -> repeat.

        The session is deliberately *synchronous*: we never send a second
        fetch until the broker has confirmed our previous ack/fail. If we
        overlapped fetches, the broker could grant us a second job while we
        were still on the first and the "one in-flight job per worker"
        invariant would break — the requeue-on-crash path could then miss the
        job the worker was *actually* running.
        """
        while not self._stop.is_set():
            self._send(protocol.fetch(self._worker_id))
            msg = protocol.recv_message(self._sock)
            mtype = msg.get("type")

            if mtype == "job":
                self._execute_job(msg)
            elif mtype == "no_job":
                # Nothing runnable for us right now (queue empty, or our
                # backoff window hasn't elapsed). Short sleep avoids a hot
                # polling loop; one sleeper per worker is cheap.
                time.sleep(0.15)
            elif mtype == "error":
                raise ConnectionError("server error: %s" % msg.get("message"))
            else:
                # ack_ok/fail_ok from a previous report — the synchronous
                # protocol means this shouldn't happen; log and continue.
                log.debug("ignoring message: %s", mtype)

    def _execute_job(self, job_msg):
        """Run one task, then wait for the broker to confirm the report.

        Waiting for ``ack_ok``/``fail_ok`` matters for crash accounting: once
        the reply arrives we know the broker has durably recorded the outcome
        (status transition written to SQLite), so it is safe to move on.
        """
        job_id = job_msg["job_id"]
        task_type = job_msg["task_type"]
        ctx = {"job_id": job_id, "worker_id": self._worker_id,
               "attempt": job_msg.get("attempt", 0)}
        log.info("executing %s (%s) attempt=%s", task_type, job_id[:8],
                 ctx["attempt"])

        ok, result = registry.execute(task_type, job_msg.get("payload"), ctx)
        if ok:
            self._send(protocol.ack(self._worker_id, job_id, result))
        else:
            log.warning("task %s (%s) failed: %s", task_type, job_id[:8],
                        result)
            self._send(protocol.fail(self._worker_id, job_id, result))
        # Consume the broker's confirmation before the next fetch.
        reply = protocol.recv_message(self._sock)
        log.debug("report for %s -> %s", job_id[:8], reply.get("type"))

    def stop(self):
        self._stop.set()
        if self._sock:
            try:
                self._sock.close()  # interrupts the blocking recv()
            except OSError:
                pass


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def build_parser():
    p = argparse.ArgumentParser(description="Distributed job queue worker")
    p.add_argument("--name", default="worker-%s" % os.getpid())
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5555)
    p.add_argument("--capabilities", nargs="*",
                   default=registry.all_types(),
                   help="task types this worker can run (default: all)")
    p.add_argument("--heartbeat-interval", type=float, default=1.0)
    p.add_argument("--server-heartbeat-timeout", type=float, default=4.0)
    p.add_argument("--verbose", action="store_true")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=(logging.DEBUG if args.verbose else
               os.getenv("DJQ_LOG_LEVEL", "INFO").upper()),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    worker = Worker(args.host, args.port, name=args.name,
                    capabilities=args.capabilities,
                    heartbeat_interval=args.heartbeat_interval,
                    server_heartbeat_timeout=args.server_heartbeat_timeout)

    def _shutdown(_sig, _frame):
        log.info("stopping worker...")
        worker.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    worker.run_forever()


if __name__ == "__main__":
    main()