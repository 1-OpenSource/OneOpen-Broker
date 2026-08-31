from __future__ import annotations

import asyncio

import pytest

from oneopen_broker.kombu_transport import register_transport

pytest.importorskip("celery")
register_transport()


def _send(port: int) -> str:
    from celery import Celery

    app = Celery("t", broker=f"oneopen://127.0.0.1:{port}")

    @app.task(name="oneopen.ping")
    def ping() -> str:
        return "pong"

    result = ping.delay()
    return result.id


@pytest.mark.celery
@pytest.mark.asyncio
async def test_celery_send_task(live_broker) -> None:
    port = live_broker.server.bound_port
    task_id = await asyncio.to_thread(_send, port)
    assert task_id
    from oneopen_broker.client.async_client import AsyncBroker

    client = AsyncBroker("127.0.0.1", port)
    await client.connect()
    queues = await client.list_queues()
    names = {q["name"] for q in queues}
    assert names
    await client.close()
