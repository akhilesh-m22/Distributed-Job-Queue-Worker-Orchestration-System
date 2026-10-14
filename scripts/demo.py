"""Start a local portfolio demo. Owns and cleans up only its child processes."""
import argparse
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import webbrowser

ROOT = Path(__file__).resolve().parents[1]


def wait_ready(url, process, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError('A service exited during startup; inspect its demo-logs file')
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, TimeoutError):
            time.sleep(.3)
    raise RuntimeError(f'Timed out waiting for {url}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workers', type=int, default=3)
    parser.add_argument('--port', type=int, default=3000, help='dashboard port')
    parser.add_argument('--api-port', type=int, default=5010)
    parser.add_argument('--broker-port', type=int, default=5555)
    parser.add_argument('--no-open', action='store_true')
    parser.add_argument('--no-ui', action='store_true')
    parser.add_argument('--db', type=Path, default=ROOT / 'var/demo.db')
    parser.add_argument('--duration', type=int, default=0, help='stop after N seconds; 0 runs until Ctrl+C')
    parser.add_argument('--production', action='store_true', help='use an existing npm build')
    args = parser.parse_args()
    if not 1 <= args.workers <= 32:
        parser.error('--workers must be between 1 and 32')
    if args.duration < 0:
        parser.error('--duration must be nonnegative')
    ports = [args.api_port, args.broker_port] + ([] if args.no_ui else [args.port])
    if len(set(ports)) != len(ports) or any(not 1 <= port <= 65535 for port in ports):
        parser.error('service ports must be distinct and between 1 and 65535')
    for port in ports:
        try:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', port))
        except OSError:
            parser.error(f'Port {port} is occupied. Stop the existing service or choose a different port.')
    node = shutil.which('node')
    next_cli = ROOT / 'frontend/node_modules/next/dist/bin/next'
    if not args.no_ui and (not node or not next_cli.exists()):
        parser.error('Install Node.js and run: npm --prefix frontend ci')
    if args.production and not (ROOT / 'frontend/.next/BUILD_ID').exists():
        parser.error('Build the dashboard first: npm --prefix frontend run build')

    logs = args.db.resolve().parent / 'demo-logs'
    logs.mkdir(parents=True, exist_ok=True)
    processes, handles = [], []
    env = dict(os.environ, PYTHONUNBUFFERED='1', DJQ_API_URL=f'http://127.0.0.1:{args.api_port}',
               DJQ_OUTPUT_DIR=str(args.db.resolve().parent / 'demo-output'))

    def launch(name, command, cwd=ROOT):
        handle = open(logs / f'{name}.log', 'w', encoding='utf-8')
        handles.append(handle)
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=handle, stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0)
        processes.append(process)
        return process

    def interrupted(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    if hasattr(signal, 'SIGBREAK'):
        signal.signal(signal.SIGBREAK, interrupted)
    try:
        broker = launch('broker', [sys.executable, '-m', 'server', '--port', str(args.broker_port),
            '--api-port', str(args.api_port), '--db', str(args.db.resolve())])
        wait_ready(f'http://127.0.0.1:{args.api_port}/ready', broker)
        for i in range(args.workers):
            launch(f'worker-{i + 1}', [sys.executable, '-m', 'worker', '--name', f'worker-{i + 1:02}', '--port', str(args.broker_port)])
        if not args.no_ui:
            if args.production:
                standalone = ROOT / 'frontend/.next/standalone'
                shutil.copytree(ROOT / 'frontend/.next/static', standalone / '.next/static', dirs_exist_ok=True)
                shutil.copytree(ROOT / 'frontend/public', standalone / 'public', dirs_exist_ok=True)
                env.update(HOSTNAME='127.0.0.1', PORT=str(args.port))
                command = [node, str(standalone / 'server.js')]
            else:
                command = [node, str(next_cli), 'dev', '--hostname', '127.0.0.1', '--port', str(args.port)]
            ui = launch('dashboard', command, ROOT / 'frontend')
            wait_ready(f'http://127.0.0.1:{args.port}', ui)
        url = f'http://127.0.0.1:{args.port}'
        print(f'\nRelay is ready: {url if not args.no_ui else "API only"}\n'
              f'{args.workers} workers | API :{args.api_port} | broker :{args.broker_port}\n'
              f'Logs: {logs}\nCtrl+C stops the demo. Queue data is preserved.\n', flush=True)
        if not args.no_open and not args.no_ui:
            webbrowser.open(url)
        deadline = time.monotonic() + args.duration if args.duration else None
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                break
            if any(p.poll() is not None for p in processes):
                raise RuntimeError(f'A service stopped. See logs in {logs}')
            time.sleep(.5)
    except KeyboardInterrupt:
        print('\nStopping demo…', flush=True)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        for process in reversed(processes):
            if process.poll() is not None:
                continue
            if os.name == 'nt':
                subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'], capture_output=True)
            else:
                process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for handle in handles:
            handle.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
