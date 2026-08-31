from __future__ import annotations

import asyncio

import pytest

from oneopen_broker.kombu_transport import register_transport

pytest.importorskip("kombu")
register_transport()


def _consume_with_drain(port: int) -> object:
    from kombu import Connection, Consumer, Exchange, Producer, Queue

    url = f"oneopen://127.0.0.1:{port}"
    received: list[object] = []

    def on_message(body, message) -> None:
        received.append(body)
        message.ack()

    with Connection(url) as conn:
        channel = conn.channel()
        exchange = Exchange("jobs", type="direct")
        queue = Queue("jobs", exchange, routing_key="jobs")
        queue.declare(channel=channel)
        Producer(channel, exchange=exchange, routing_key="jobs").publish(
            {"n": 1}, serializer="json"
        )
        consumer = Consumer(channel, [queue], callbacks=[on_message], accept=["json"])
        consumer.consume()
        conn.drain_events(timeout=8)
        consumer.cancel()
    return received[0] if received else None


@pytest.mark.celery
@pytest.mark.asyncio
async def test_kombu_drain_events(live_broker) -> None:
    port = live_broker.server.bound_port
    body = await asyncio.to_thread(_consume_with_drain, port)
    assert body == {"n": 1}
