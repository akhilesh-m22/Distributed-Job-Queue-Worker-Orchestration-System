"""Task registry.

Workers do not know *how* to run a task at compile time; they look it up here
by name at dispatch time. That decouples "which task types exist" (a single
registry) from "which worker handles which types" (per-worker capabilities).
The same contract lets the server do capability-based routing: a worker
advertises ``capabilities=["send_email"]`` and the server only hands it jobs
of those types.
"""

import random

REGISTRY = {}


def register(name):
    """Decorator: add a callable to the global task registry."""
    def decorator(fn):
        REGISTRY[name] = fn
        return fn
    return decorator


def all_types():
    return sorted(REGISTRY)


def execute(name, payload, ctx):
    """Run a task; returns ``(ok: bool, result)`` where result is the task's
    return value on success or a human-readable error string on failure.

    The worker reports failures to the server, which owns the retry policy.
    This function never raises into the worker loop.
    """
    fn = REGISTRY.get(name)
    if fn is None:
        return False, "unknown task_type: %r (registered: %s)" % (
            name, ", ".join(all_types())
        )
    try:
        return True, fn(payload, ctx)
    except Exception as exc:  # noqa: BLE001 - intentionally catch-all
        return False, "%s: %s" % (type(exc).__name__, exc)


def injectable_failure(payload, ctx, probability=0.0):
    """Deterministic + probabilistic failure injection used by the example
    tasks so you can *demonstrate* retries, dead-lettering and recovery:

    * ``payload.fail_first_n``  -> raise on the first N attempts.
    * ``payload.fail_probability`` -> raise with P on every attempt.

    ``ctx.attempt`` is the attempt counter maintained by the server (starts at
    1 for the first execution), so ``fail_first_n=2`` fails attempts 1 and 2
    and succeeds on attempt 3 — a perfect showcase for exponential backoff.
    """
    first_n = int(payload.get("fail_first_n", 0))
    prob = payload.get("fail_probability", probability)
    if ctx["attempt"] <= first_n:
        raise TaskInjectedFailure(
            "injected failure on attempt %d (fail_first_n=%d)"
            % (ctx["attempt"], first_n)
        )
    if prob and random.random() < prob:
        raise TaskInjectedFailure(
            "injected random failure on attempt %d (p=%.2f)"
            % (ctx["attempt"], prob)
        )


class TaskInjectedFailure(Exception):
    """Thrown by injectable_failure to simulate a task that throws at runtime."""