"""Stress tests. Marked so they can be skipped in fast runs with -m 'not stress'."""

from __future__ import annotations

import asyncio

import pytest

from oneopen_broker.client.async_client import AsyncBroker
from oneopen_broker.core.message import Message
from oneopen_broker.core.queue import QueueEngine
from oneopen_broker.core.results import EnqueueResult


@pytest.mark.stress
def test_100k_ready_messages() -> None:
    eng = QueueEngine(max_length=None)
    eng.declare_queue("big")
    for i in range(100_000):
        result = eng.enqueue(Message.create("big", b"x"))
        assert result is EnqueueResult.OK
    assert eng.queue_info("big")["ready"] == 100_000
    n = 0
    while eng.reserve("big", "c") is not None:
        n += 1
        if n >= 1000:
            break
    assert n == 1000


@pytest.mark.asyncio
@pytest.mark.stress
async def test_rapid_connect(live_broker) -> None:
    port = live_broker.server.bound_port

    async def once() -> None:
        c = AsyncBroker("127.0.0.1", port)
        await c.connect()
        await c.ping()
        await c.close()

    await asyncio.gather(*[once() for _ in range(50)])
