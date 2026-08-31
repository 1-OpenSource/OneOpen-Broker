from __future__ import annotations

import pytest

from oneopen_broker.broker.config import AuthUser, load_config
from oneopen_broker.client.async_client import AsyncBroker
from oneopen_broker.client.connection import BrokerError
from oneopen_broker.protocol.errors import FORBIDDEN, UNAUTHORIZED
from oneopen_broker.security.acl import authorize
from oneopen_broker.security.auth import Principal, authenticate
from oneopen_broker.security.tls import client_ssl_context, server_ssl_context


def test_token_auth_timing_safe() -> None:
    users = [AuthUser(name="app", token="s3cret", role="producer")]
    principal = authenticate(token="s3cret", username=None, password=None, users=users)
    assert principal is not None
    assert principal.role == "producer"
    assert authenticate(token="wrong", username=None, password=None, users=users) is None


def test_authorize_requires_auth() -> None:
    ok, code, _ = authorize(None, "PUBLISH", auth_required=True)
    assert not ok
    assert code == UNAUTHORIZED
    ok, _, _ = authorize(None, "PING", auth_required=True)
    assert ok
    ok, _, _ = authorize(None, "AUTH", auth_required=True)
    assert ok


def test_role_forbidden() -> None:
    producer = Principal("app", "producer")
    ok, code, _ = authorize(producer, "DLQ_REQUEUE", auth_required=True)
    assert not ok
    assert code == FORBIDDEN
    ok, _, _ = authorize(producer, "PUBLISH", auth_required=True)
    assert ok
    admin = Principal("ops", "admin")
    ok, _, _ = authorize(admin, "DLQ_REQUEUE", auth_required=True)
    assert ok


def test_tls_context_requires_certs() -> None:
    class Tls:
        enabled = True
        certfile = ""
        keyfile = ""
        cafile = ""
        require_client_cert = False

    with pytest.raises(ValueError):
        server_ssl_context(Tls())
    assert client_ssl_context(enabled=False) is None


def test_security_yaml(tmp_path, monkeypatch) -> None:
    path = tmp_path / "broker.yaml"
    path.write_text(
        "security:\n  auth:\n    enabled: true\n    users:\n"
        "      - name: ops\n        token: abc\n        role: admin\n",
        encoding="utf-8",
    )
    cfg = load_config(path)
    assert cfg.security.auth.enabled is True
    assert cfg.security.auth.users[0].name == "ops"
    monkeypatch.setenv("ONEOPEN_AUTH_TOKEN", "from-env")
    cfg = load_config(path)
    assert any(u.token == "from-env" for u in cfg.security.auth.users)


@pytest.mark.asyncio
async def test_auth_required_rejects_anonymous(tmp_path) -> None:
    from oneopen_broker.broker.lifecycle import Broker

    cfg = load_config(
        overrides={
            "server.port": 0,
            "persistence.directory": str(tmp_path / "data"),
            "persistence.fsync": "none",
            "persistence.snapshot_interval": 0,
            "persistence.snapshot_on_shutdown": False,
        }
    )
    cfg.security.auth.enabled = True
    cfg.security.auth.users = [AuthUser(name="app", token="good-token", role="admin")]
    broker = Broker(cfg)
    await broker.start()
    port = broker.server.bound_port
    try:
        anon = AsyncBroker("127.0.0.1", port)
        await anon.connect()
        with pytest.raises(BrokerError) as exc:
            await anon.publish(b"x", queue="secure")
        assert exc.value.code == UNAUTHORIZED
        await anon.close()

        authed = AsyncBroker("127.0.0.1", port, token="good-token")
        await authed.connect()
        message_id = await authed.publish(b"ok", queue="secure")
        assert message_id
        await authed.close()
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_acl_blocks_monitor_from_publish(tmp_path) -> None:
    from oneopen_broker.broker.lifecycle import Broker

    cfg = load_config(
        overrides={
            "server.port": 0,
            "persistence.directory": str(tmp_path / "data"),
            "persistence.fsync": "none",
            "persistence.snapshot_interval": 0,
            "persistence.snapshot_on_shutdown": False,
        }
    )
    cfg.security.auth.enabled = True
    cfg.security.auth.users = [
        AuthUser(name="ops", token="see-only", role="monitor"),
        AuthUser(name="app", token="write", role="producer"),
    ]
    broker = Broker(cfg)
    await broker.start()
    port = broker.server.bound_port
    try:
        monitor = AsyncBroker("127.0.0.1", port, token="see-only")
        await monitor.connect()
        stats = await monitor.stats()
        assert "connections" in stats
        with pytest.raises(BrokerError) as exc:
            await monitor.publish(b"nope", queue="secure")
        assert exc.value.code == FORBIDDEN
        await monitor.close()

        producer = AsyncBroker("127.0.0.1", port, token="write")
        await producer.connect()
        await producer.publish(b"yes", queue="secure")
        await producer.close()
    finally:
        await broker.shutdown()
