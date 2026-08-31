from __future__ import annotations

import asyncio

import pytest

from oneopen_broker.client.async_client import AsyncBroker


@pytest.mark.asyncio
async def test_publish_consume_ack(client: AsyncBroker) -> None:
    await client.publish(b"hello", queue="tasks")
    msg = await asyncio.wait_for(client.consume("tasks"), timeout=2)
    assert msg.payload == b"hello"
    await msg.ack()
    empty = await client.get("tasks")
    assert empty is None


@pytest.mark.asyncio
async def test_nack_redelivers(client: AsyncBroker) -> None:
    await client.publish(b"retry-me", queue="jobs")
    msg = await asyncio.wait_for(client.consume("jobs"), timeout=2)
    await msg.nack(requeue=True)
    await asyncio.sleep(1.1)
    again = await asyncio.wait_for(client.consume("jobs"), timeout=2)
    assert again.payload == b"retry-me"
    assert again.attempts >= 1
    await again.ack()


@pytest.mark.asyncio
async def test_ping_and_stats(client: AsyncBroker) -> None:
    assert await client.ping() is True
    stats = await client.stats()
    assert "connections" in stats
    assert stats["connections"] >= 1
