"""Recovery tests: the two at-least-once requeue pathways.

1. **Socket EOF**: worker disappears mid-job (crash, kill). The server's
   per-connection reader gets EOF and requeues the job immediately.
2. **Heartbeat timeout**: the socket stays open but the worker stops sending
   heartbeats (machine hangs, process wedged). The monitor requeues it.
3. **Server restart**: jobs left ``running`` survive a broker crash because
   state lives in SQLite; on boot they are requeued.
"""

import time

from server.store import STATUS_QUEUED, Store
from server.queue_server import QueueServer
from tests.helpers import RawWorker, wait_for

# The slow task keeps a worker busy long enough for us to kill it.
SLOW = {"delay_secs": 60, "width": 1, "height": 1}


def test_eof_recovery_requeues_job(api, store, server, worker_factory,
                                   submit, job_done):
    """A worker that vanishes without acking must not lose its job."""
    worker_factory(capabilities=["send_email"])  # a healthy second worker

    job_id = submit("send_email", {"to": "lost@example.com", "delay_secs": 0.2})

    # Pathological worker claims the job and crashes before acking.
    raw = RawWorker(server.host, server.port, name="crashy",
                    capabilities=["send_email"])
    assert raw.fetch() == job_id
    raw.close()  # abrupt EOF, no ack/fail

    # Job is immediately requeued by the EOF handler, then the real worker
    # claims and completes it.
    assert wait_for(lambda: store.get_job(job_id)["status"] == STATUS_QUEUED)
    assert job_done(job_id, "succeeded")
    body = api.get("/jobs/%s" % job_id).get_json()
    # Two executions happened (claim by crashy + claim by the survivor).
    assert body["attempts"] == 2
    assert body["status"] == "succeeded"


def test_heartbeat_timeout_requeues_job(api, store, server, worker_factory,
                                        submit, job_done):
    """A worker that goes silent (socket still open) is detected by the
    monitor from missing heartbeats and its job is requeued."""
    worker_factory(capabilities=["send_email"])
    job_id = submit("send_email", {"to": "stuck@example.com", "delay_secs": 0.3})

    raw = RawWorker(server.host, server.port, name="zombie",
                    capabilities=["send_email"])
    assert raw.fetch() == job_id
    # Zombie: keep the socket open but never heartbeat, never ack.

    # The monitor (heartbeat_timeout=0.6s in fixtures) must declare it dead.
    assert wait_for(
        lambda: any(w["name"] == "zombie" and w["status"] == "dead"
                    for w in server.worker_snapshots()),
        timeout=5.0,
    )
    # And the job must have come back to the queue for the survivor.
    assert wait_for(lambda: store.get_job(job_id)["status"] == STATUS_QUEUED)
    assert job_done(job_id, "succeeded")
    raw.close()


def test_live_worker_heartbeats_are_not_mistaken_for_dead(api, server,
                                                          worker_factory,
                                                          submit, job_done):
    """A healthy worker with a long-running job must never get requeued."""
    worker_factory(capabilities=["resize_image"], heartbeat_interval=0.05)
    job_id = submit("resize_image", {"delay_secs": 0.4, "width": 2, "height": 2})
    # Give the monitor ample time to fire the requeue if it were wrong.
    time.sleep(1.5)
    assert all(w["status"] == "connected" for w in server.worker_snapshots())
    assert job_done(job_id, "succeeded")
    assert api.get("/jobs/%s" % job_id).get_json()["attempts"] == 1


def test_server_startup_recovers_running_jobs(tmp_path):
    """Broker dies mid-flight; a fresh broker on the same DB recovers.

    The in-flight job lives only in SQLite, never in broker memory, so a
    restart finds the job STILL in ``running`` and re-queues it on boot.
    """

    db = str(tmp_path / "recover.db")

    # Broker #1: a job is claimed (attempts=1, status=running) by a worker
    # that then dies together with the broker — no cleanup code ever ran.
    store1 = Store(db, max_attempts=3)
    job = store1.create_job("resize_image", {"delay_secs": 0.1, "width": 1,
                                             "height": 1})
    assert store1.claim_next(["resize_image"], "worker-that-died")["id"] == job["id"]
    assert store1.get_job(job["id"])["status"] == "running"
    store1.close()  # "crash": connection closed without any cleanup

    # Broker #2 boot: recover_unfinished() (called from QueueServer.start)
    # returns the orphaned job to the queue.
    store2 = Store(db, max_attempts=3)
    assert store2.recover_unfinished() == 1
    assert store2.get_job(job["id"])["status"] == STATUS_QUEUED

    # And a real cluster can now finish it end-to-end.
    server2 = QueueServer(store2, host="127.0.0.1", port=0, heartbeat_timeout=0.4)
    server2.start(background=True)
    from worker.worker import Worker
    from tests.helpers import SpawnedWorker
    sw = SpawnedWorker(
        Worker(server2.host, server2.port, name="resurrected",
               capabilities=["resize_image"], heartbeat_interval=0.1),
        server2,
    ).spawn()
    assert wait_for(lambda: store2.get_job(job["id"])["status"] == "succeeded",
                    timeout=10.0)
    sw.stop()
    server2.stop()
    store2.close()


def test_error_message_path(server):
    """Unknown worker ids get a polite error reply, never a silent hang."""
    raw = RawWorker(server.host, server.port, name="probe")
    from common import protocol
    protocol.send(raw.sock, protocol.fetch("worker-id-that-does-not-exist"))
    reply = protocol.recv_message(raw.sock)
    assert reply["type"] == "error"
    raw.close()