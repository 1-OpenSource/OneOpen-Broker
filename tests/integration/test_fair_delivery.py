from __future__ import annotations

import asyncio

import pytest

from oneopen_broker.client.async_client import AsyncBroker


@pytest.mark.asyncio
async def test_two_consumers_fair(live_broker) -> None:
    port = live_broker.server.bound_port
    pub = AsyncBroker("127.0.0.1", port)
    a = AsyncBroker("127.0.0.1", port)
    b = AsyncBroker("127.0.0.1", port)
    await pub.connect()
    await a.connect()
    await b.connect()

    results: list[bytes] = []

    async def take(broker: AsyncBroker) -> None:
        for _ in range(3):
            msg = await asyncio.wait_for(broker.consume("fair", prefetch=1), timeout=5)
            results.append(msg.payload)
            await msg.ack()

    ta = asyncio.create_task(take(a))
    tb = asyncio.create_task(take(b))
    await asyncio.sleep(0.1)
    for i in range(6):
        await pub.publish(bytes([i]), queue="fair")
    await asyncio.wait_for(asyncio.gather(ta, tb), timeout=8)
    assert len(results) == 6
    await pub.close()
    await a.close()
    await b.close()
