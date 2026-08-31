"""oneopen-broker command line."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from oneopen_broker import __version__
from oneopen_broker.broker.config import load_config
from oneopen_broker.broker.lifecycle import run_broker
from oneopen_broker.broker.logging import setup_logging
from oneopen_broker.cli import consumers as consumers_cmd
from oneopen_broker.cli import dlq as dlq_cmd
from oneopen_broker.cli import pubsub as pubsub_cmd
from oneopen_broker.cli import queues as queues_cmd
from oneopen_broker.cli import stats as stats_cmd
from oneopen_broker.cli import top as top_cmd
from oneopen_broker.client.sync_client import Broker


def _add_conn(p: argparse.ArgumentParser) -> None:
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6380)
    p.add_argument("--token", help="auth token (or ONEOPEN_AUTH_TOKEN)")
    p.add_argument("--tls", action="store_true")
    p.add_argument("--tls-ca")
    p.add_argument("--tls-insecure", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="oneopen-broker")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    start = sub.add_parser("start", help="start the broker")
    start.add_argument("--config", "-c")
    start.add_argument("--host")
    start.add_argument("--port", type=int)
    start.add_argument("--data-dir")
    start.add_argument("--fsync", choices=["always", "everysec", "none"])
    start.add_argument("--log-level", default="INFO")
    start.add_argument("--auth-token", help="require this admin token")
    start.add_argument("--tls-cert")
    start.add_argument("--tls-key")
    start.add_argument("--tls-ca")

    stats = sub.add_parser("stats", help="broker stats")
    _add_conn(stats)

    queues = sub.add_parser("queues", help="list queues")
    _add_conn(queues)

    queue = sub.add_parser("queue", help="queue details")
    _add_conn(queue)
    queue.add_argument("name")

    cons = sub.add_parser("consumers", help="list consumers")
    _add_conn(cons)

    ch = sub.add_parser("channels", help="list pub/sub channels")
    _add_conn(ch)

    dlq = sub.add_parser("dlq", help="inspect or requeue DLQ")
    _add_conn(dlq)
    dlq.add_argument("queue")
    dlq.add_argument("--requeue")

    top = sub.add_parser("top", help="live monitor")
    _add_conn(top)
    top.add_argument("--interval", type=float, default=1.0)
    return parser


def _connect(args: argparse.Namespace) -> Broker:
    token = args.token or os.environ.get("ONEOPEN_AUTH_TOKEN")
    return Broker(
        args.host,
        args.port,
        token=token,
        ssl=bool(args.tls),
        ssl_cafile=args.tls_ca or "",
        ssl_insecure=bool(args.tls_insecure),
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "start":
        setup_logging(args.log_level)
        overrides = {}
        if args.host:
            overrides["server.host"] = args.host
        if args.port:
            overrides["server.port"] = args.port
        if args.data_dir:
            overrides["persistence.directory"] = args.data_dir
        if args.fsync:
            overrides["persistence.fsync"] = args.fsync
        cfg = load_config(args.config, overrides=overrides)
        if args.auth_token:
            from oneopen_broker.broker.config import AuthUser

            cfg.security.auth.enabled = True
            cfg.security.auth.users.append(
                AuthUser(name="cli", token=args.auth_token, role="admin")
            )
        if args.tls_cert and args.tls_key:
            cfg.security.tls.enabled = True
            cfg.security.tls.certfile = args.tls_cert
            cfg.security.tls.keyfile = args.tls_key
        if args.tls_ca:
            cfg.security.tls.cafile = args.tls_ca
        setup_logging(cfg.logging.level)
        try:
            asyncio.run(run_broker(cfg))
        except KeyboardInterrupt:
            return 0
        return 0

    client = _connect(args)
    try:
        if args.command == "stats":
            stats_cmd.print_stats(client)
        elif args.command == "queues":
            queues_cmd.print_queues(client)
        elif args.command == "queue":
            queues_cmd.print_queue(client, args.name)
        elif args.command == "consumers":
            consumers_cmd.print_consumers(client)
        elif args.command == "channels":
            pubsub_cmd.print_channels(client)
        elif args.command == "dlq":
            dlq_cmd.print_dlq(client, args.queue, args.requeue)
        elif args.command == "top":
            top_cmd.run_top(client, args.interval)
        else:
            parser.error("unknown command")
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
