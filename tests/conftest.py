from __future__ import annotations

import pytest

from oneopen_broker.broker.config import load_config
from oneopen_broker.broker.lifecycle import Broker
from oneopen_broker.client.async_client import AsyncBroker


@pytest.fixture
async def live_broker(tmp_path):
    cfg = load_config(
        overrides={
            "server.host": "127.0.0.1",
            "server.port": 0,
            "persistence.directory": str(tmp_path / "data"),
            "persistence.fsync": "none",
            "persistence.snapshot_interval": 0,
            "persistence.snapshot_on_shutdown": False,
            "queues.default_visibility_timeout": 30.0,
        }
    )
    broker = Broker(cfg)
    await broker.start()
    try:
        yield broker
    finally:
        await broker.shutdown()


@pytest.fixture
async def client(live_broker):
    port = live_broker.server.bound_port
    broker = AsyncBroker("127.0.0.1", port)
    await broker.connect()
    try:
        yield broker
    finally:
        await broker.close()
