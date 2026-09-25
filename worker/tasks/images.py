"""Example task: mock image resize.

Simulates a CPU/IO heavy operation (real pipelines use Pillow/ffmpeg or a
dedicated microservice) with a short sleep and burns a few CPU cycles by
"downsampling" a synthetic pixel buffer. Supports failure injection via the
standard ``payload`` contract (see ``registry.injectable_failure``).
"""

import random
import time
import uuid

from worker.tasks.registry import register, injectable_failure


@register("resize_image")
def resize_image(payload, ctx):
    """Resize a synthetic image from (w,h) to (nw,nh); returns output meta."""
    injectable_failure(payload, ctx)
    width = int(payload.get("width", 1920))
    height = int(payload.get("height", 1080))
    new_width = int(payload.get("new_width", 320))
    new_height = int(payload.get("new_height", 240))
    output_key = payload.get("output_key") or uuid.uuid4().hex

    # Emulate a few hundred ms of real pixel crunching.
    pixels = random.Random(output_key).random() * (width * height)
    for _ in range(2000):
        pixels = (pixels * 31 + 17) % 1_000_000_007
    time.sleep(payload.get("delay_secs", random.uniform(0.05, 0.3)))

    return {
        "output_key": output_key,
        "input_size": "%dx%d" % (width, height),
        "output_size": "%dx%d" % (new_width, new_height),
        "ratio": round(new_width / max(width, 1), 4),
        "pixels_hash": pixels % 2 ** 32,
    }