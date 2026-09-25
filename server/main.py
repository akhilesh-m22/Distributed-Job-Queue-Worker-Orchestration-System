"""Entrypoint: run the TCP queue server + heartbeat monitor + REST API.

Usage::

    python -m server [--port 5555] [--api-port 5010] [--db var/queue.db]

The REST API runs as a daemon thread inside this process so it can talk to
the in-memory worker registry and the shared SQLite store without a second
IPC hop.
"""

import argparse
import logging
import os
import signal
import sys

from server import store as store_mod
from server.queue_server import QueueServer


def build_parser():
    p = argparse.ArgumentParser(description="Distributed job queue server")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5555)
    p.add_argument("--api-port", type=int, default=5010)
    p.add_argument("--db", default=os.getenv("DJQ_DB", "var/queue.db"))
    p.add_argument("--heartbeat-timeout", type=float, default=4.0)
    p.add_argument("--max-attempts", type=int, default=5)
    p.add_argument("--backoff-base", type=float, default=1.0)
    p.add_argument("--backoff-cap", type=float, default=30.0)
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--no-api", action="store_true", help="disable the REST API")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=(logging.DEBUG if args.verbose else
               os.getenv("DJQ_LOG_LEVEL", "INFO").upper()),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    os.makedirs(os.path.dirname(args.db) or ".", exist_ok=True)
    store = store_mod.Store(
        args.db, max_attempts=args.max_attempts,
        backoff_base=args.backoff_base, backoff_cap=args.backoff_cap,
    )
    server = QueueServer(store, host=args.host, port=args.port,
                         heartbeat_timeout=args.heartbeat_timeout)
    server.start(background=False)

    api_thread = None
    if not args.no_api:
        from api.server import run_api
        _app, api_thread = run_api(server, host=args.host, port=args.api_port)
        print("API listening on http://%s:%s" % (args.host, args.api_port))

    def shutdown(_sig, _frame):
        logging.getLogger().info("shutting down...")
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    try:
        # serve_forever() blocks in the accept loop on the main thread.
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
        store.close()


if __name__ == "__main__":
    main()