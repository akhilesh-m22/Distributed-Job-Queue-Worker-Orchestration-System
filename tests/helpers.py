"""Shared test helpers: waiting, raw protocol clients for failure injection."""

import socket
import threading
import time

from common import protocol


def wait_for(predicate, timeout=8.0, interval=0.05):
    """Poll ``predicate()`` until it returns a truthy value or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


class SpawnedWorker:
    """Runs a ``Worker`` on a background thread (used to fake a real TCP peer).

    ``spawn()`` returns once the worker has successfully registered with the
    server, so tests can rely on worker presence before submitting jobs.
    """

    def __init__(self, worker, server):
        self.worker = worker
        self.server = server
        self.thread = None

    def spawn(self):
        self.thread = threading.Thread(target=self.worker.run_forever,
                                       daemon=True)
        self.thread.start()
        assert wait_for(
            lambda: any(w["name"] == self.worker.name
                        for w in self.server.worker_snapshots())
        ), "worker %s never registered" % self.worker.name
        return self

    def stop(self):
        self.worker.stop()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=3)


class RawWorker:
    """A bare TCP client speaking our protocol but *without* the worker loop.

    Lets tests simulate pathological behaviours a real worker never does on
    purpose: claiming a job and then vanishing (EOF), or going silent while
    keeping the socket open (heartbeat timeout path).
    """

    def __init__(self, host, port, name="raw", capabilities=None, pid=0):
        self.sock = socket.create_connection((host, port), timeout=10)
        protocol.send(self.sock, protocol.register(name, pid, capabilities or []))
        self.worker_id = protocol.recv_message(self.sock)["worker_id"]

    def fetch(self):
        protocol.send(self.sock, protocol.fetch(self.worker_id))
        msg = protocol.recv_message(self.sock)
        assert msg["type"] == "job", "expected a job, got %r" % msg
        return msg["job_id"]

    def ack(self, job_id, result=None):
        protocol.send(self.sock, protocol.ack(self.worker_id, job_id, result))

    def fail(self, job_id, error="boom"):
        protocol.send(self.sock, protocol.fail(self.worker_id, job_id, error))

    def heartbeat(self):
        protocol.send(self.sock, protocol.heartbeat(self.worker_id))

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass