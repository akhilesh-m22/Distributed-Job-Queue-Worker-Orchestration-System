"""Unit tests for the length-prefixed wire protocol."""

import socket

import pytest

from common import protocol


@pytest.fixture
def pair():
    a, b = socket.socketpair()
    yield a, b
    a.close()
    b.close()


def test_roundtrip(pair):
    a, b = pair
    msg = {"type": "job", "job_id": "abc", "payload": {"n": 1},
           "attempt": 2}
    protocol.send(a, msg)
    assert protocol.recv_message(b) == msg


def test_multiple_frames_stay_ordered(pair):
    a, b = pair
    for m in ({"type": "no_job"}, {"type": "job", "i": 0},
              {"type": "ack"}, {"type": "heartbeat"}):
        protocol.send(a, m)
    for m in ({"type": "no_job"}, {"type": "job", "i": 0},
              {"type": "ack"}, {"type": "heartbeat"}):
        assert protocol.recv_message(b) == m


def test_partial_and_garbage(pair):
    """TCP can deliver bytes in arbitrary chunks; framing must be robust."""
    a, b = pair
    raw = protocol.pack_message({"type": "job", "value": "hello world"})
    # Deliver one byte at a time to simulate worst-case fragmentation.
    for byte in raw:
        a.sendall(bytes([byte]))
    assert protocol.recv_message(b) == {"type": "job", "value": "hello world"}

    # Garbage inside the frame body should raise ProtocolError.
    a.sendall(b"\x00\x00\x00\x04" + b"\xff\xff\xff\xff")
    with pytest.raises(protocol.ProtocolError):
        protocol.recv_message(b)


def test_eof_raises(pair):
    a, b = pair
    a.close()
    with pytest.raises(ConnectionResetError):
        protocol.recv_message(b)


def test_oversized_frame_rejected(pair):
    a, b = pair
    a.sendall(b"\xff\xff\xff\xff")  # claims 4 GiB frame
    with pytest.raises(protocol.ProtocolError):
        protocol.recv_message(b)