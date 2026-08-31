from __future__ import annotations

import asyncio

import pytest

from oneopen_broker.client.async_client import AsyncBroker


@pytest.mark.asyncio
async def test_pubsub_fanout(client: AsyncBroker, live_broker) -> None:
    port = live_broker.server.bound_port
    sub_a = AsyncBroker("127.0.0.1", port)
    sub_b = AsyncBroker("127.0.0.1", port)
    await sub_a.connect()
    await sub_b.connect()
    got_a: list[bytes] = []
    got_b: list[bytes] = []

    async def collect(broker: AsyncBroker, bucket: list[bytes]) -> None:
        async for event in broker.subscribe("events"):
            bucket.append(event.payload)
            if len(bucket) >= 1:
                break

    ta = asyncio.create_task(collect(sub_a, got_a))
    tb = asyncio.create_task(collect(sub_b, got_b))
    await asyncio.sleep(0.1)
    n = await client.publish_channel("events", b"hello-all")
    assert n >= 2
    await asyncio.wait_for(asyncio.gather(ta, tb), timeout=2)
    assert got_a == [b"hello-all"]
    assert got_b == [b"hello-all"]
    await sub_a.close()
    await sub_b.close()
