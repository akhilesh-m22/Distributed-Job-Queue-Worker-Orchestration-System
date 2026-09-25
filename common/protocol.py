"""Wire protocol for the queue server <-> worker TCP channel.

All messages are JSON objects. Every message is framed with a 4-byte
big-endian length prefix followed by the UTF-8 encoded JSON body, e.g.::

    b"\\x00\\x00\\x00\\x2A" + b'{"type": "heartbeat", "worker_id": "..."}'

Design notes (interview-ready):
------------------------------
* **Length-prefixed framing** means both sides can call ``recv()`` exactly as
  many bytes as the stream needs, so a partial message is never mistaken for
  a complete one (TCP delivers a byte stream, not discrete packets).
* **Big-endian** so two machines with different byte order agree on the
  length without negotiation.
* **JSON** keeps the protocol debuggable (you can eyeball a captured frame)
  and is fast enough for a job queue where each frame is a small object.
"""

import json
import socket
import struct

#: 4-byte big-endian integer that precedes every JSON payload.
_FRAME_HEADER = struct.Struct("!I")


class ProtocolError(Exception):
    """Raised when a frame cannot be parsed (malformed length/data)."""


def pack_message(message):
    """Serialize ``message`` (dict) into a single framed byte string."""
    body = json.dumps(message).encode("utf-8")
    return _FRAME_HEADER.pack(len(body)) + body


def _read_exact(sock, n):
    """Read exactly ``n`` bytes from ``sock`` or raise on EOF."""
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionResetError("Connection closed by peer")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_message(sock):
    """Read and deserialize the next framed message from ``sock``.

    Blocks until a full frame is available. Raises ``ProtocolError`` on
    garbage and lets ``ConnectionResetError``/``socket.timeout`` bubble up.
    """
    try:
        header = _read_exact(sock, _FRAME_HEADER.size)
    except ConnectionResetError:
        raise
    (length,) = _FRAME_HEADER.unpack(header)
    if length > 16 * 1024 * 1024:
        raise ProtocolError("frame too large: %d bytes" % length)
    body = _read_exact(sock, length)
    try:
        return json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ProtocolError("malformed JSON frame: %s" % exc)


# ---------------------------------------------------------------------------
# Convenience constructors shared by server and workers.
# ---------------------------------------------------------------------------

def register(name, pid, capabilities):
    return {"type": "register", "name": name, "pid": pid, "capabilities": capabilities}


def heartbeat(worker_id):
    return {"type": "heartbeat", "worker_id": worker_id}


def fetch(worker_id):
    return {"type": "fetch", "worker_id": worker_id}


def ack(worker_id, job_id, result):
    return {"type": "ack", "worker_id": worker_id, "job_id": job_id, "result": result}


def fail(worker_id, job_id, error):
    return {"type": "fail", "worker_id": worker_id, "job_id": job_id, "error": error}


def unregister(worker_id):
    return {"type": "unregister", "worker_id": worker_id}


def registered(worker_id):
    return {"type": "registered", "worker_id": worker_id}


def grant_job(job_id, task_type, payload, attempt):
    return {"type": "job", "job_id": job_id, "task_type": task_type,
            "payload": payload, "attempt": attempt}


def no_job():
    return {"type": "no_job"}


def server_error(message):
    return {"type": "error", "message": message}


def send(sock, message, send_lock=None):
    """Thread-safe frame send. Pass ``send_lock`` when multiple threads write."""
    if send_lock is not None:
        with send_lock:
            sock.sendall(pack_message(message))
    else:
        sock.sendall(pack_message(message))