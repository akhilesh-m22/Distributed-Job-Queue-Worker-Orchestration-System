"""Unit tests for the SQLite persistence layer (retry/requeue state machine)."""

from server.store import (STATUS_DEAD_LETTER, STATUS_FAILED, STATUS_QUEUED,
                          STATUS_RUNNING, STATUS_SUCCEEDED)


def make_job(store, task_type="send_email", priority=0, **payload_kwargs):
    payload = {"to": "x@example.com", **payload_kwargs}
    return store.create_job(task_type, payload, priority=priority)


def force_due(store, job_id):
    """Tests only: rewind a retry job's clock so it is claimable right now."""
    store._conn.execute(
        "UPDATE jobs SET scheduled_at = '2000-01-01 00:00:00.000000' "
        "WHERE id = ?", (job_id,))


def test_create_job_defaults(store):
    job = make_job(store)
    assert job["status"] == STATUS_QUEUED
    assert job["attempts"] == 0
    assert job["max_attempts"] == 3
    assert job["worker_id"] is None
    assert store.get_job(job["id"])["id"] == job["id"]


def test_claim_marks_running_and_increments_attempts(store):
    job = make_job(store)
    claimed = store.claim_next(["send_email"], "worker-1")
    assert claimed["id"] == job["id"]
    assert claimed["status"] == STATUS_RUNNING
    assert claimed["attempts"] == 1
    assert claimed["worker_id"] == "worker-1"

    # While running it must not be claimable by anyone else.
    assert store.claim_next(["send_email"], "worker-2") is None


def test_claim_respects_capabilities(store):
    store.create_job("send_email", {})
    store.create_job("resize_image", {})
    # A worker that only knows about email must only ever get the email job.
    got = store.claim_next(["send_email"], "w1")
    assert got["task_type"] == "send_email"
    # The image job stays queued because no qualified worker exists.
    assert store.claim_next(["send_email"], "w1") is None
    assert store.claim_next(["resize_image"], "w2")["task_type"] == "resize_image"


def test_priority_ordering(store):
    low = store.create_job("send_email", {}, priority=0)
    high = store.create_job("send_email", {}, priority=10)
    first = store.claim_next(["send_email"], "w")
    assert first["id"] == high["id"]
    second = store.claim_next(["send_email"], "w")
    assert second["id"] == low["id"]


def test_claim_respects_backoff_schedule(store):
    job = make_job(store)
    store.claim_next(["send_email"], "w1")
    store.fail_job(job["id"], "w1", "boom")  # schedules retry in the future
    # Not due yet -> nothing claimable.
    assert store.claim_next(["send_email"], "w1") is None
    assert store.get_job(job["id"])["status"] == STATUS_FAILED
    # Backoff eventually elapses -> claimable again.
    force_due(store, job["id"])
    claimed = store.claim_next(["send_email"], "w1")
    assert claimed is not None and claimed["attempts"] == 2


def test_ack_success_then_stale_ack_rejected(store):
    job = make_job(store)
    store.claim_next(["send_email"], "w1")
    assert store.ack_job(job["id"], "w1", {"sent": True}) is True
    assert store.get_job(job["id"])["status"] == STATUS_SUCCEEDED

    # At-least-once hazard: the same job completed "again" elsewhere. The
    # duplicate ack must not corrupt state.
    assert store.ack_job(job["id"], "w1", {"sent": True}) is False
    assert store.ack_job(job["id"], "some-other-worker", {}) is False


def test_retry_until_dead_letter(store):
    job = make_job(store)
    statuses = []
    for i in range(1, 4):  # max_attempts=3 from fixture
        assert store.claim_next(["send_email"], "w")["id"] == job["id"]
        outcome = store.fail_job(job["id"], "w", "disk full (%d)" % i)
        statuses.append(outcome)
        force_due(store, job["id"])  # let the backoff window elapse
    assert [o["retry"] for o in statuses] == [True, True, False]
    assert statuses[0]["attempt"] == 1
    assert statuses[0]["retry_at"]  # a backoff time was scheduled
    assert statuses[2]["dead_letter"] is True
    assert store.get_job(job["id"])["status"] == STATUS_DEAD_LETTER
    assert store.get_job(job["id"])["attempts"] == 3
    assert "disk full (3)" == store.get_job(job["id"])["last_error"]


def test_dead_letter_requeue_resets_attempts(store):
    job = make_job(store)
    for _ in range(3):
        store.claim_next(["send_email"], "w")
        store.fail_job(job["id"], "w", "boom")
        force_due(store, job["id"])
    assert store.get_job(job["id"])["status"] == STATUS_DEAD_LETTER

    assert store.retry_dead_letter(job["id"]) is True
    fresh = store.get_job(job["id"])
    assert fresh["status"] == STATUS_QUEUED
    assert fresh["attempts"] == 0
    # Full retry budget again.
    claimed = store.claim_next(["send_email"], "w")
    assert claimed["id"] == job["id"] and claimed["attempts"] == 1


def test_requeue_preserves_attempts_not_in_error_budget(store):
    """Recovery requeues (worker crash) must NOT consume the retry budget."""
    job = make_job(store)
    store.claim_next(["send_email"], "w1")
    assert store.requeue_job(job["id"], "worker died", ) is True

    requeued = store.get_job(job["id"])
    assert requeued["status"] == STATUS_QUEUED
    assert requeued["attempts"] == 1  # kept the claim, no extra fail counted
    assert store.requeue_job(job["id"], "again") is False  # already queued


def test_recover_unfinished_on_restart(store):
    job = make_job(store)
    store.claim_next(["send_email"], "w1")
    assert store.get_job(job["id"])["status"] == STATUS_RUNNING

    # Simulate the server crashing *after* handing out the job.
    assert store.recover_unfinished() == 1
    recovered = store.get_job(job["id"])
    assert recovered["status"] == STATUS_QUEUED
    assert recovered["worker_id"] is None
    # And again: nothing running, so no-op.
    assert store.recover_unfinished() == 0


def test_counts_and_list(store):
    s1 = make_job(store)
    s2 = make_job(store, task_type="resize_image")
    store.claim_next(["send_email"], "w")
    c = store.counts()
    assert c["total"] == 2
    assert c[STATUS_RUNNING] == 1
    assert c[STATUS_QUEUED] == 1

    email_jobs = store.list_jobs(task_type="send_email")
    assert [j["id"] for j in email_jobs] == [s1["id"]]
    assert store.list_jobs(status=STATUS_FAILED) == []
    assert store.list_jobs(limit=1)  # returns newest first
    assert store.get_job(s2["id"])["task_type"] == "resize_image"


def test_backoff_grows_exponentially(store):
    """Each retry must wait strictly longer than the previous one."""
    from server.store import exponential_backoff
    samples_a = [exponential_backoff(1, base=0.05, cap=0.5) for _ in range(200)]
    samples_b = [exponential_backoff(2, base=0.05, cap=0.5) for _ in range(200)]
    assert max(samples_a) < min(samples_b)  # attempt 2 waits ~2x attempt 1


def test_failed_job_scheduled_in_future(store):
    """A failed (retryable) job must not be claimable until its backoff."""
    job = make_job(store)
    store.claim_next(["send_email"], "w")
    outcome = store.fail_job(job["id"], "w", "boom")
    assert outcome["retry"] is True
    failed = store.get_job(job["id"])
    assert failed["scheduled_at"] > failed["started_at"]
    # And the retry_at returned matches what we stored.
    assert outcome["retry_at"] == failed["scheduled_at"]