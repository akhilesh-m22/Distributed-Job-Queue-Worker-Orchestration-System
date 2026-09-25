"""API surface tests: submission, status query, worker listing, dead-letter,
and input validation."""

from tests.helpers import wait_for


def test_submit_job_validation(api):
    assert api.post("/jobs", json={}).status_code == 400
    assert api.post("/jobs", json={"task_type": ""}).status_code == 400
    assert api.post("/jobs", json={"task_type": 42}).status_code == 400
    assert api.post("/jobs", json={"task_type": "send_email", "payload": {}}).status_code == 202


def test_post_job_returns_queued_and_creates_record(api, store):
    r = api.post("/jobs", json={"task_type": "send_email",
                                "payload": {"to": "x@example.com"},
                                "priority": 5})
    body = r.get_json()
    assert body["status"] == "queued"
    job = store.get_job(body["job_id"])
    assert job["task_type"] == "send_email"
    assert job["payload"] == {"to": "x@example.com"}
    assert job["priority"] == 5
    assert job["status"] == "queued"


def test_get_job_404(api):
    assert api.get("/jobs/nope").status_code == 404


def test_get_job_exposes_all_fields(api, store):
    job = store.create_job("send_email", {"to": "a@b.c"})
    r = api.get("/jobs/%s" % job["id"])
    body = r.get_json()
    assert body["job_id"] == job["id"]
    assert body["status"] == "queued"
    assert body["attempts"] == 0
    assert body["created_at"] is not None
    assert body["finished_at"] is None
    assert body["worker_id"] is None
    assert body["payload"] == {"to": "a@b.c"}


def test_workers_endpoint_reflects_connections(api, worker_factory):
    worker_factory(name="alice", capabilities=["send_email"])
    worker_factory(name="bob", capabilities=["resize_image"])
    workers = api.get("/workers").get_json()["workers"]
    names = {w["name"] for w in workers}
    assert {"alice", "bob"} <= names
    alice = next(w for w in workers if w["name"] == "alice")
    assert alice["status"] == "connected"
    assert alice["capabilities"] == ["send_email"]
    assert alice["current_job"] is None
    assert alice["pid"] > 0
    assert "heartbeat_age_secs" in alice


def test_workers_show_current_job_while_running(api, store, worker_factory,
                                                submit, job_done):
    worker_factory(name="busy", capabilities=["send_email"])
    # A task long enough to observe mid-flight worker state.
    job_id = submit("send_email", {"to": "z@w.com", "delay_secs": 0.5,
                                   "fail_first_n": 0})
    assert wait_for(
        lambda: any(w["name"] == "busy" and w["current_job"] for w in
                    api.get("/workers").get_json()["workers"])
    )
    assert job_done(job_id, "succeeded")
    assert all(w["current_job"] is None for w in api.get("/workers").get_json()["workers"])


def test_dead_letter_flow_through_api(api, worker_factory, submit, job_done):
    worker_factory(capabilities=["send_email"])
    doomed = submit("send_email", {"fail_first_n": 99})
    assert job_done(doomed, "dead-letter")

    dl = api.get("/dead-letter").get_json()
    assert [j["job_id"] for j in dl["jobs"]] == [doomed]
    assert dl["jobs"][0]["last_error"]

    r = api.post("/dead-letter/%s/retry" % doomed)
    assert r.status_code == 200
    assert r.get_json()["status"] == "queued"

    # Once requeued, retrying it again is a client error.
    assert api.post("/dead-letter/%s/retry" % doomed).status_code == 404
    # Missing ids are also 404 (and unknown ones don't crash).
    assert api.post("/dead-letter/does-not-exist/retry").status_code == 404


def test_list_jobs_filter(api, store):
    store.create_job("send_email", {"to": "1@x.com"})
    store.create_job("resize_image", {})
    assert api.get("/jobs").get_json()["count"] == 2
    e = api.get("/jobs", query_string={"task_type": "send_email"})
    assert e.get_json()["count"] == 1
    assert e.get_json()["jobs"][0]["task_type"] == "send_email"
    q = api.get("/jobs", query_string={"status": "queued"})
    assert q.get_json()["count"] == 2


def test_health_and_stats(api, store):
    assert api.get("/health").get_json()["status"] == "ok"
    store.create_job("send_email", {})
    stats = api.get("/stats").get_json()
    assert stats["queued"] == 1
    assert stats["total"] == 1
    assert "workers" in stats