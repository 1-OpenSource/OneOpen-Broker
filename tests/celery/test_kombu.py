from __future__ import annotations

import asyncio

import pytest

from oneopen_broker.kombu_transport import register_transport

pytest.importorskip("kombu")
pytest.importorskip("celery")
register_transport()


def _produce_consume(port: int) -> None:
    from kombu import Connection, Exchange, Producer, Queue
    from kombu.simple import SimpleQueue

    url = f"oneopen://127.0.0.1:{port}"
    with Connection(url) as conn:
        channel = conn.channel()
        exchange = Exchange("celery", type="direct")
        queue = Queue("celery", exchange, routing_key="celery")
        queue.declare(channel=channel)
        producer = Producer(channel, exchange=exchange, routing_key="celery")
        producer.publish({"task": "demo"}, serializer="json")
        simple = SimpleQueue(channel, queue)
        message = simple.get(timeout=5)
        assert message is not None
        message.ack()
        simple.close()


@pytest.mark.celery
@pytest.mark.asyncio
async def test_kombu_produce_consume(live_broker) -> None:
    port = live_broker.server.bound_port
    await asyncio.to_thread(_produce_consume, port)
