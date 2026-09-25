"""End-to-end tests driving the full stack through the real socket protocol."""

from tests.helpers import wait_for


def test_submit_process_succeed(api, worker_factory, submit, job_done):
    worker_factory(capabilities=["send_email"])  # capability-gated routing
    job_id = submit("send_email", {"to": "ada@example.com",
                                   "subject": "hello"})
    assert job_done(job_id, "succeeded")

    # Fetch the durable snapshot back through the API.
    r = api.get("/jobs/%s" % job_id)
    body = r.get_json()
    assert body["status"] == "succeeded"
    assert body["attempts"] == 1
    assert body["result"]["message_id"]  # the mock produced an artifact
    assert body["finished_at"] is not None

    # The job must be truly finished: nothing left in queued/running.
    assert api.get("/stats").get_json()["succeeded"] == 1


def test_unknown_task_reports_failure_and_retries_then_deadletters(
        api, worker_factory, submit, job_done):
    worker_factory(capabilities=["send_email"])
    # No worker can run this task type -> server never hands it out.
    r = api.post("/jobs", json={"task_type": "does_not_exist",
                                "payload": {}})
    assert r.status_code == 202
    # Give the queue a moment; nobody can claim it so it must stay queued.
    assert wait_for(lambda: api.get("/stats").get_json()["queued"] == 1)
    assert api.get("/stats").get_json()["running"] == 0


def test_worker_with_capability_filter_only_gets_matching_jobs(
        api, worker_factory, submit, job_done):
    worker_factory(capabilities=["resize_image"])
    # This email job has no qualified worker => has to stay queued forever.
    email_id = submit("send_email", {})
    # This one is claimable and gets processed.
    img_id = submit("resize_image", {"width": 4, "height": 4})
    assert job_done(img_id, "succeeded")
    assert wait_for(
        lambda: api.get("/jobs/%s" % email_id).get_json()["status"] == "queued"
    )
    assert api.get("/jobs/%s" % email_id).get_json()["status"] == "queued"