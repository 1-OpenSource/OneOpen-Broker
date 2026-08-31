"""Celery task issuance benchmark. Requires a running broker and worker."""

from __future__ import annotations

import argparse
import time

import oneopen_broker

oneopen_broker.register_kombu()

from celery import Celery


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("-n", type=int, default=1000)
    p.add_argument("--broker", default="oneopen://127.0.0.1:6380")
    args = p.parse_args()
    app = Celery("bench", broker=args.broker)

    @app.task
    def ping() -> str:
        return "pong"

    t0 = time.perf_counter()
    for _ in range(args.n):
        ping.delay()
    elapsed = time.perf_counter() - t0
    print(f"submitted={args.n} elapsed={elapsed:.3f}s rate={args.n / elapsed:.0f}/s")


if __name__ == "__main__":
    main()
