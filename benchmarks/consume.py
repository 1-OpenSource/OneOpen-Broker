"""Consume throughput helper. Start the broker and a publisher first."""

from __future__ import annotations

import argparse
import asyncio
import time

from oneopen_broker import AsyncBroker


async def run(host: str, port: int, n: int, queue: str) -> None:
    async with AsyncBroker(host, port) as broker:
        t0 = time.perf_counter()
        for _ in range(n):
            msg = await broker.consume(queue)
            await msg.ack()
        elapsed = time.perf_counter() - t0
    print(f"consumed={n} elapsed={elapsed:.3f}s rate={n / elapsed:.0f}/s")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6380)
    p.add_argument("-n", type=int, default=10000)
    p.add_argument("--queue", default="bench")
    args = p.parse_args()
    asyncio.run(run(args.host, args.port, args.n, args.queue))


if __name__ == "__main__":
    main()
