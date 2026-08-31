from __future__ import annotations

import asyncio

import pytest

from oneopen_broker.client.async_client import AsyncBroker


@pytest.mark.asyncio
async def test_direct_and_fanout(client: AsyncBroker) -> None:
    await client.declare_queue("qa")
    await client.declare_queue("qb")
    await client.declare_exchange("images", "direct")
    await client.bind("images", "qa", "jpg")
    await client.publish(b"pic", exchange="images", routing_key="jpg")
    msg = await asyncio.wait_for(client.consume("qa"), timeout=2)
    assert msg.payload == b"pic"
    await msg.ack()

    await client.declare_exchange("news", "fanout")
    await client.bind("news", "qa", "")
    await client.bind("news", "qb", "")
    await client.publish(b"flash", exchange="news", routing_key="ignored")
    a = await asyncio.wait_for(client.consume("qa"), timeout=2)
    b = await asyncio.wait_for(client.consume("qb"), timeout=2)
    assert a.payload == b"flash"
    assert b.payload == b"flash"
    await a.ack()
    await b.ack()
