"""Publish throughput helper. Start the broker first."""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time

from oneopen_broker import AsyncBroker


async def run(host: str, port: int, n: int, queue: str) -> None:
    async with AsyncBroker(host, port) as broker:
        latencies: list[float] = []
        t0 = time.perf_counter()
        for i in range(n):
            s = time.perf_counter()
            await broker.publish(b"x" * 64, queue=queue)
            latencies.append(time.perf_counter() - s)
        elapsed = time.perf_counter() - t0
    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    print(f"published={n} elapsed={elapsed:.3f}s rate={n / elapsed:.0f}/s")
    print(f"p50={p50 * 1000:.2f}ms p95={p95 * 1000:.2f}ms p99={p99 * 1000:.2f}ms")
    print(f"mean={statistics.mean(latencies) * 1000:.2f}ms")


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
