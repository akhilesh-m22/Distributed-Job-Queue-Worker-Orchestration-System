"""Load test: burst submissions + a mid-flight worker kill + full-drain assert.

This is the acceptance test for the reliability guarantees:

* submit a large burst of jobs **concurrently**,
* kill a worker **while it is processing a job** (the socket is wrenched away
  with no chance to ACK),
* assert that every single job eventually completes exactly once (status flips
  to ``succeeded`` exactly one time per job) with **zero loss**, and
* print a summary report: throughput, retries triggered, recovery time, and
  how many duplicate executions at-least-once delivery caused.

Modes
-----
* **cluster** (default): spawns a fresh queue server + N workers, runs the
  test, then tears them all down — ``python scripts/load_test.py`` is a
  one-shot demo.
* **attach**: point it at an already-running server with
  ``--attach HOST --server-port P --api-port P2`` so you can stress an
  existing deployment (workers must already be connected).

Usage
-----
    python scripts/load_test.py --jobs 300 --workers 4
"""

import argparse
import concurrent.futures as futures
import datetime
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

import requests

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# --------------------------------------------------------------------------- #
# Bookkeeping helpers
# --------------------------------------------------------------------------- #
def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Cluster:
    """Spawns/owns the server and worker subprocesses for a run.

    Uses a fresh SQLite file per run and deletes it on teardown, so a load
    test never inherits state from an earlier run (which would otherwise
    pollute execution/attempt statistics).
    """

    def __init__(self, worker_count):
        self.server_port = free_port()
        self.api_port = free_port()
        self.worker_count = worker_count
        self.procs = []
        self._workdir = tempfile.mkdtemp(prefix="djq-loadtest-")
        self.db_path = os.path.join(self._workdir, "queue.db")

    def start(self):
        env = dict(os.environ)
        # Spawned children quiet down to WARNING so the report stays readable.
        env["DJQ_LOG_LEVEL"] = "WARNING"
        server = subprocess.Popen(
            [sys.executable, "-m", "server",
             "--port", str(self.server_port),
             "--api-port", str(self.api_port),
             "--db", self.db_path],
            cwd=REPO_ROOT, env=env,
        )
        workers = []
        for i in range(self.worker_count):
            workers.append(subprocess.Popen(
                [sys.executable, "-m", "worker",
                 "--host", "127.0.0.1", "--port", str(self.server_port),
                 "--name", "load-worker-%d" % i],
                cwd=REPO_ROOT, env=env,
            ))
        self.procs = [server] + workers

    def stop(self):
        for p in self.procs:
            try:
                p.terminate()
            except OSError:
                pass
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                try:
                    p.kill()
                except OSError:
                    pass
        self.procs = []
        try:
            shutil.rmtree(self._workdir, ignore_errors=True)
        except OSError:
            pass


# --------------------------------------------------------------------------- #
# The test
# --------------------------------------------------------------------------- #
def wait_for(predicate, timeout, interval=0.25, desc="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def submit_job(base, task_type, payload, priority=0):
    r = requests.post(base + "/jobs", json={
        "task_type": task_type, "payload": payload, "priority": priority,
    }, timeout=10)
    r.raise_for_status()
    return r.json()["job_id"]


def run(args):
    base = "http://127.0.0.1:%d" % args.api_port
    cluster = None

    if not args.attach:
        cluster = Cluster(args.workers)
        print("[1/6] spawning cluster: server:%s api:%s workers:%d"
              % (cluster.server_port, cluster.api_port, args.workers))
        cluster.start()
        base = "http://127.0.0.1:%d" % cluster.api_port
    else:
        print("[1/6] attaching to %s:%s (api :%s)"
              % (args.attach, args.server_port, args.api_port))

    try:
        print("[2/6] waiting for API + workers ...")
        assert wait_for(
            lambda: healthy(base), args.wait_timeout,
            desc="api up",
        ), "API did not come up in time"
        assert wait_for(
            lambda: connected_workers(base) >= args.workers,
            args.wait_timeout, desc="%d workers connected" % args.workers,
        ), "not enough workers connected in time"
        print("       ^ connected workers: %s"
              % [w["name"] for w in workers(base)])

        # --- Phase A: prime a few slow jobs so a worker is busy when we kill.
        print("[3/6] submitting %d slow jobs (delay %.0fs) to pin a worker"
              % (args.slow_jobs, args.slow_delay))
        slow_ids = [submit_job(base, "resize_image",
                               {"width": 4096, "height": 4096,
                                "output_key": "slow-%d" % i,
                                "delay_secs": args.slow_delay})
                    for i in range(args.slow_jobs)]

        print("       ^ waiting for a worker to pick one up ...")
        assert wait_for(
            lambda: any(w["current_job"] for w in workers(base)),
            args.wait_timeout, desc="a busy worker",
        ), "no worker picked up the slow job in time"

        # --- Phase B: kill the busiest worker mid-processing.
        victim = next(w for w in workers(base) if w["current_job"])
        print("[4/6] KILLING worker %s (pid=%s) mid-job %s"
              % (victim["name"], victim["pid"], victim["current_job"]))
        killed_job = victim["current_job"]
        t_kill = time.monotonic()
        try:
            os.kill(victim["pid"], signal.SIGTERM)  # hard kill; no ACK possible
        except ProcessLookupError:
            print("       ^ worker pid already gone (race); continuing")

        # --- Phase C: burst of concurrent cheap jobs.
        print("[5/6] submitting burst of %d jobs ..." % args.jobs)
        pool_size = min(args.jobs, 50)
        with futures.ThreadPoolExecutor(max_workers=pool_size) as pool:
            futs = []
            for i in range(args.jobs):
                task = random.choice(["send_email", "resize_image",
                                      "generate_report"])
                payload = {"id": i}
                if task == "send_email":
                    payload = {"to": "user%d@example.com" % i,
                               "subject": "hello %d" % i, "delay_secs": 0.02}
                elif task == "generate_report":
                    payload = {"rows": 200, "delay_secs": 0.03}
                else:
                    payload = {"width": 1920, "height": 1080, "delay_secs": 0.04}
                # A fraction of jobs fail on the first attempt to exercise the
                # exponential-backoff retry path and show up in the report.
                if random.random() < args.fail_fraction:
                    payload["fail_first_n"] = random.randint(1, 2)
                futs.append(pool.submit(submit_job, base, task, payload))
            burst_ids = [f.result(timeout=120) for f in futs]

        all_ids = slow_ids + burst_ids
        print("       ^ %d total jobs (%d slow + %d burst) submitted"
              % (len(all_ids), len(slow_ids), len(burst_ids)))

        # --- Phase D: wait for drain and verify no loss.
        print("[6/6] waiting for full drain, zero-loss check ...")
        ok = wait_for(
            lambda: drained(base, set(all_ids)),
            args.drain_timeout, interval=0.5, desc="all jobs finished",
        )
        t_done = time.monotonic()
        results = fetch_all(base, set(all_ids))
        verify_zero_loss(all_ids, killed_job, results)

        report(args, base, all_ids, results, victim)
        return 0 if ok and len(results) == len(all_ids) else 1
    finally:
        if cluster:
            print("\ntearing down cluster ...")
            cluster.stop()


# --------------------------------------------------------------------------- #
# Verification & report
# --------------------------------------------------------------------------- #
def healthy(base):
    try:
        return requests.get(base + "/health", timeout=2).status_code == 200
    except requests.RequestException:
        return False


def workers(base):
    try:
        return requests.get(base + "/workers", timeout=5).json().get("workers", [])
    except requests.RequestException:
        return []


def connected_workers(base):
    return sum(1 for w in workers(base) if w["status"] == "connected")


def drained(base, ids):
    results = fetch_all(base, ids)
    return results and all(
        r["status"] in ("succeeded", "dead-letter") for r in results.values()
    )


def fetch_all(base, ids, chunk=200):
    """Fetch job details for a large id set efficiently via ?status-less list."""
    r = requests.get(base + "/jobs", params={"limit": 10000}, timeout=10)
    r.raise_for_status()
    by_id = {j["job_id"]: j for j in r.json()["jobs"]}
    return {i: by_id[i] for i in ids if i in by_id}


def verify_zero_loss(all_ids, killed_job, results):
    missing = [i for i in all_ids if i not in results]
    dead = [j for j in results.values() if j["status"] == "dead-letter"]
    not_done = [j["job_id"] for j in results.values()
                if j["status"] != "succeeded"]
    assert not missing, "LOST jobs (never recorded): %s" % missing
    assert not dead, "jobs reached dead-letter unexpectedly: %s" % dead
    assert not not_done, "jobs not completed: %s" % not_done
    # Exactly-once *completion*: every submitted job reached succeeded.
    print("       ^ ZERO LOSS: all %d jobs succeeded exactly once" % len(all_ids))
    if killed_job in results:
        print("       ^ victim's job %s was recovered and completed by %s"
              % (killed_job[:8], results[killed_job]["worker_id"]))


def report(args, base, all_ids, results, victim):
    n = len(all_ids)
    succeeded = sum(1 for j in results.values() if j["status"] == "succeeded")
    dead_letter = sum(1 for j in results.values() if j["status"] == "dead-letter")
    queued = sum(1 for j in results.values() if j["status"] == "queued")
    running = sum(1 for j in results.values() if j["status"] == "running")
    executions = sum(j["attempts"] for j in results.values())
    retried = sum(1 for j in results.values() if j["attempts"] > 1)
    drain_secs = max(0.0, tstamp_diff(results))
    killed = results.get(victim.get("current_job"))
    recovery_secs = span_secs(killed) if killed else 0.0

    print("\n========================  LOAD TEST REPORT  ========================")
    print("cluster:           %d workers, %d submitted jobs"
          % (args.workers, n))
    print("burst concurrency: %d parallel submitters" % min(n, 50))
    print("kill:              worker %s (pid %s) killed mid-job %s"
          % (victim["name"], victim["pid"], (victim["current_job"] or "?")[:8]))
    print("-------------------------------------------------------------------")
    print("final status:      succeeded=%d dead-letter=%d queued=%d running=%d"
          % (succeeded, dead_letter, queued, running))
    print("zero loss:         %s" % ("YES" if succeeded == n else "NO"))
    print("total executions:  %d (attempts across this run's claims)"
          % executions)
    print("duplicate execs:   %d (at-least-once cost: re-claims + retries)"
          % max(0, executions - n))
    print("jobs that retried: %d (attempts > 1)" % retried)
    print("throughput:        %.1f jobs/sec (submission -> full drain)"
          % (n / max(drain_secs, 1e-6)))
    print("recovery time:     %.1fs (victim job requeued + completed)"
          % recovery_secs)
    print("===================================================================\n")


def span_secs(job):
    """Seconds between a job's created_at and finished_at (0 if unfinished)."""
    if job is None:
        return 0.0
    if not (job.get("created_at") and job.get("finished_at")):
        return 0.0
    return _interval_secs(job["created_at"], job["finished_at"])


def tstamp_diff(results):
    """Seconds between earliest created_at and latest finished_at in results."""
    times = []
    for j in results.values():
        if j.get("created_at"):
            times.append(_parse_ts(j["created_at"]))
        if j.get("finished_at"):
            times.append(_parse_ts(j["finished_at"]))
    if not times:
        return 0.0
    return (max(times) - min(times)).total_seconds()


def _parse_ts(ts):
    return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _interval_secs(start, end):
    return max(0.0, (_parse_ts(end) - _parse_ts(start)).total_seconds())


def build_parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--jobs", type=int, default=200)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--slow-jobs", type=int, default=4, help="slow jobs that pin a worker")
    p.add_argument("--slow-delay", type=float, default=15.0, help="seconds each slow job runs")
    p.add_argument("--fail-fraction", type=float, default=0.1,
                   help="fraction of burst jobs that fail on first attempt(s)")
    p.add_argument("--attach", default=None, help="attach to a running server host")
    p.add_argument("--server-port", type=int, default=5555)
    p.add_argument("--api-port", type=int, default=5010)
    p.add_argument("--wait-timeout", type=float, default=60)
    p.add_argument("--drain-timeout", type=float, default=180)
    return p


if __name__ == "__main__":
    sys.exit(run(build_parser().parse_args()))