"""Pytest fixtures: a real in-process queue server + API + worker pool."""

import pytest

from api.server import create_app
from server.queue_server import QueueServer
from server.store import Store
from tests.helpers import SpawnedWorker, wait_for


@pytest.fixture
def store(tmp_path):
    """SQLite store with fast retry timings so tests are quick."""
    s = Store(str(tmp_path / "test.db"), max_attempts=3,
              backoff_base=0.05, backoff_cap=0.5)
    yield s
    s.close()


@pytest.fixture
def server(store):
    """A real TCP queue server bound to an ephemeral port."""
    qs = QueueServer(store, host="127.0.0.1", port=0,
                     heartbeat_timeout=0.6, monitor_interval=0.05)
    qs.start(background=True)
    yield qs
    qs.stop()


@pytest.fixture
def api(server):
    """Flask test client wired to the running server."""
    return create_app(server).test_client()


@pytest.fixture
def worker_factory(server):
    """Spawn real worker processes (threads) that talk TCP to the server."""
    spawned = []

    def _spawn(name="test-worker", capabilities=None, heartbeat_interval=0.1):
        from worker.worker import Worker
        w = Worker(server.host, server.port, name=name,
                   capabilities=capabilities,
                   heartbeat_interval=heartbeat_interval)
        sw = SpawnedWorker(w, server).spawn()
        spawned.append(sw)
        return sw

    yield _spawn
    for sw in spawned:
        sw.stop()


@pytest.fixture
def submit(api):
    """Submit a job through the real REST API and return its id."""
    def _submit(task_type, payload=None, priority=0, max_attempts=None):
        body = {"task_type": task_type, "payload": payload or {},
                "priority": priority}
        if max_attempts is not None:
            body["max_attempts"] = max_attempts
        r = api.post("/jobs", json=body)
        assert r.status_code == 202, r.data
        return r.get_json()["job_id"]
    return _submit


@pytest.fixture
def job_done(store):
    def _done(job_id, status="succeeded", timeout=15.0):
        def check():
            job = store.get_job(job_id)
            return job and job["status"] == status
        return wait_for(check, timeout=timeout)
    return _done