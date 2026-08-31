"""Config loading tests."""

from __future__ import annotations

from pathlib import Path

from oneopen_broker.broker.config import load_config


def test_defaults() -> None:
    cfg = load_config()
    assert cfg.server.port == 6380
    assert cfg.persistence.fsync == "everysec"
    assert cfg.queues.default_max_attempts == 3


def test_yaml_and_env(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "broker.yaml"
    path.write_text(
        "server:\n  port: 7000\npersistence:\n  fsync: always\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("ONEOPEN_SERVER_HOST", "0.0.0.0")
    cfg = load_config(path)
    assert cfg.server.port == 7000
    assert cfg.server.host == "0.0.0.0"
    assert cfg.persistence.fsync == "always"


def test_cli_overrides() -> None:
    cfg = load_config(overrides={"server.port": 9000})
    assert cfg.server.port == 9000
