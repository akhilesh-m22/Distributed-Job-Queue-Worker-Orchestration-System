"""Tests for the retry + exponential backoff + dead-letter state machine,
exercised end-to-end through the API and real workers."""

from tests.helpers import wait_for


def test_transient_failure_auto_retries_then_succeeds(
        api, worker_factory, submit, job_done):
    """A job that fails its first two attempts must succeed on the third.

    With the fixture's max_attempts=3, the sequence is:
    claim(1) fail -> backoff, claim(2) fail -> backoff, claim(3) succeed.
    """
    worker_factory(capabilities=["send_email"])
    job_id = submit(
        "send_email",
        {"to": "grace@example.com", "fail_first_n": 2},     # fail 1..2
    )
    assert job_done(job_id, "succeeded")
    body = api.get("/jobs/%s" % job_id).get_json()
    assert body["attempts"] == 3
    assert body["max_attempts"] == 3
    # The last_error field should hold the final injected failure (attempt 2).
    assert "injected failure" in body["last_error"]


def test_retry_waits_at_least_a_backoff_between_attempts(api, worker_factory,
                                                         submit, store):
    """A retried job must leave the queue for at least its backoff window."""
    worker_factory(capabilities=["send_email"])
    job_id = submit("send_email", {"fail_first_n": 2})
    wait_for(lambda: (store.get_job(job_id) or {}).get("attempts", 0) == 2)
    import time
    t0 = time.monotonic()
    assert wait_for(
        lambda: (store.get_job(job_id) or {}).get("attempts", 0) == 3,
        timeout=10.0,
    )
    elapsed = time.monotonic() - t0
    # Attempt-2's backoff = base=0.05 * 2^1 + jitter, so the gap while the job
    # was *not* claimable is at least ~0.1s. Allow CI slop but reject a
    # retry that happened with zero waiting (a backoff bug).
    assert elapsed >= 0.08, "retry happened before the backoff window"


def test_always_failing_job_reaches_dead_letter_then_can_be_requeued(
        api, worker_factory, submit, job_done):
    worker_factory(capabilities=["send_email"])

    # A job that fails on every attempt burns the whole budget (3).
    doomed = submit("send_email", {"fail_first_n": 99})
    assert job_done(doomed, "dead-letter")
    body = api.get("/jobs/%s" % doomed).get_json()
    assert body["status"] == "dead-letter"
    assert body["attempts"] == 3
    assert body["last_error"]  # the last error message is preserved

    # The dead-letter API lists it.
    listed = api.get("/dead-letter").get_json()
    ids = [j["job_id"] for j in listed["jobs"]]
    assert doomed in ids

    # Operator requeues it. It returns to the queue with a fresh budget but
    # the same failure condition, so it dead-letters a second time.
    r = api.post("/dead-letter/%s/retry" % doomed)
    assert r.status_code == 200
    assert api.get("/jobs/%s" % doomed).get_json()["status"] in ("queued", "running")
    assert job_done(doomed, "dead-letter")
    requeued_body = api.get("/jobs/%s" % doomed).get_json()
    assert requeued_body["attempts"] == 3  # fresh budget fully consumed again

    # Requeueing a non-dead-letter job is a 404.
    second = api.post("/jobs", json={"task_type": "send_email",
                                     "payload": {"fail_first_n": 99}})
    second_id = second.get_json()["job_id"]
    assert wait_for(lambda: api.get("/jobs/%s" % second_id)
                    .get_json()["status"] == "dead-letter")
    gone = api.post("/dead-letter/%s/retry" % second_id)
    assert gone.status_code == 200
    # It was just requeued, so retrying again is now invalid.
    again = api.post("/dead-letter/%s/retry" % second_id)
    assert again.status_code == 404