"""The launcher must leave no listening services after a bounded demo."""
import os
from pathlib import Path
import socket
import subprocess
import sys


def test_demo_starts_and_cleans_up_owned_processes(tmp_path):
    reserved = [socket.socket(), socket.socket()]
    for sock in reserved:
        sock.bind(('127.0.0.1', 0))
    api_port, broker_port = [sock.getsockname()[1] for sock in reserved]
    for sock in reserved:
        sock.close()
    root = Path(__file__).resolve().parents[1]
    run = subprocess.run([
        sys.executable, str(root / 'scripts/demo.py'), '--no-ui', '--no-open',
        '--workers', '2', '--api-port', str(api_port), '--broker-port', str(broker_port),
        '--db', str(tmp_path / 'queue.db'), '--duration', '1',
    ], cwd=root, env=dict(os.environ), capture_output=True, text=True, timeout=40)
    assert run.returncode == 0, run.stdout + run.stderr
    assert 'Relay is ready' in run.stdout
    for port in (api_port, broker_port):
        with socket.socket() as sock:
            assert sock.connect_ex(('127.0.0.1', port)) != 0
