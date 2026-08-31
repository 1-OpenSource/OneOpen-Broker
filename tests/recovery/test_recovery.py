from __future__ import annotations

import asyncio

import pytest

from oneopen_broker.broker.config import load_config
from oneopen_broker.broker.lifecycle import Broker
from oneopen_broker.client.async_client import AsyncBroker
from oneopen_broker.persistence.records import encode_record


@pytest.mark.asyncio
async def test_recover_after_publish(tmp_path) -> None:
    data = tmp_path / "data"
    cfg = load_config(
        overrides={
            "server.port": 0,
            "persistence.directory": str(data),
            "persistence.fsync": "always",
            "persistence.snapshot_interval": 0,
            "persistence.snapshot_on_shutdown": True,
        }
    )
    broker = Broker(cfg)
    await broker.start()
    port = broker.server.bound_port
    client = AsyncBroker("127.0.0.1", port)
    await client.connect()
    await client.publish(b"durable", queue="persist")
    await client.close()
    await broker.shutdown()

    broker2 = Broker(cfg)
    await broker2.start()
    port2 = broker2.server.bound_port
    client2 = AsyncBroker("127.0.0.1", port2)
    await client2.connect()
    msg = await asyncio.wait_for(client2.consume("persist"), timeout=2)
    assert msg.payload == b"durable"
    await msg.ack()
    await client2.close()
    await broker2.shutdown()


@pytest.mark.asyncio
async def test_partial_aof_truncated(tmp_path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    aof = data / "appendonly.aof"
    rec = encode_record(
        1,
        "QUEUE_DECLARE",
        {
            "name": "q",
            "durable": True,
            "max_length": None,
            "visibility_timeout": 30,
            "max_attempts": 3,
            "dead_letter": True,
        },
    )
    rec2 = encode_record(
        2,
        "MESSAGE_PUBLISH",
        {
            "id": "mid-1",
            "queue": "q",
            "attempts": 0,
            "max_attempts": 3,
            "priority": 0,
            "created_at": 1.0,
            "available_at": None,
            "headers": None,
            "exchange": "",
            "routing_key": "q",
        },
        b"keep-me",
    )
    aof.write_bytes(rec + rec2 + b"\x00\x01truncated")
    cfg = load_config(
        overrides={
            "server.port": 0,
            "persistence.directory": str(data),
            "persistence.fsync": "none",
            "persistence.snapshot_interval": 0,
            "persistence.snapshot_on_shutdown": False,
        }
    )
    broker = Broker(cfg)
    await broker.start()
    client = AsyncBroker("127.0.0.1", broker.server.bound_port)
    await client.connect()
    msg = await asyncio.wait_for(client.consume("q"), timeout=2)
    assert msg.payload == b"keep-me"
    await msg.ack()
    await client.close()
    await broker.shutdown()
