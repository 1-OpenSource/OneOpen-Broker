"""Broker configuration: YAML file, ONEOPEN_* environment, CLI overrides."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml

DEFAULT_PORT = 6380
DEFAULT_MAX_FRAME_SIZE = 16_777_216


@dataclass(slots=True)
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = DEFAULT_PORT


@dataclass(slots=True)
class PersistenceConfig:
    directory: str = "./data"
    fsync: str = "everysec"
    snapshot_interval: int = 300
    snapshot_on_shutdown: bool = True

    def __post_init__(self) -> None:
        if self.fsync not in {"always", "everysec", "none"}:
            raise ValueError("persistence.fsync must be always, everysec, or none")


@dataclass(slots=True)
class QueuesConfig:
    default_visibility_timeout: float = 300.0
    default_max_attempts: int = 3
    default_max_length: int | None = None
    retry_backoff: str = "exponential"
    retry_base_delay: float = 1.0
    recent_ack_limit: int = 100_000

    def __post_init__(self) -> None:
        if self.retry_backoff not in {"fixed", "exponential"}:
            raise ValueError("queues.retry_backoff must be fixed or exponential")


@dataclass(slots=True)
class NetworkConfig:
    max_connections: int = 10_000
    max_frame_size: int = DEFAULT_MAX_FRAME_SIZE
    heartbeat_interval: float = 30.0
    outbound_buffer: int = 1000
    idle_timeout: float = 120.0


@dataclass(slots=True)
class PubSubConfig:
    subscriber_buffer: int = 1000


@dataclass(slots=True)
class LoggingConfig:
    level: str = "INFO"


@dataclass(slots=True)
class TlsConfig:
    enabled: bool = False
    certfile: str = ""
    keyfile: str = ""
    cafile: str = ""
    require_client_cert: bool = False


@dataclass(slots=True)
class AuthUser:
    name: str
    token: str = ""
    password: str = ""
    role: str = "admin"

    def __post_init__(self) -> None:
        if self.role not in {"admin", "producer", "consumer", "subscriber", "monitor"}:
            raise ValueError(f"unknown role {self.role!r}")
        if not self.token and not self.password:
            raise ValueError(f"user {self.name!r} needs a token or password")


@dataclass(slots=True)
class AuthConfig:
    enabled: bool = False
    users: list[AuthUser] = field(default_factory=list)


@dataclass(slots=True)
class SecurityConfig:
    tls: TlsConfig = field(default_factory=TlsConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)


@dataclass(slots=True)
class BrokerConfig:
    server: ServerConfig = field(default_factory=ServerConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    queues: QueuesConfig = field(default_factory=QueuesConfig)
    network: NetworkConfig = field(default_factory=NetworkConfig)
    pubsub: PubSubConfig = field(default_factory=PubSubConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_ENV_MAP: list[tuple[str, str, str, type]] = [
    ("ONEOPEN_SERVER_HOST", "server", "host", str),
    ("ONEOPEN_SERVER_PORT", "server", "port", int),
    ("ONEOPEN_PERSISTENCE_DIRECTORY", "persistence", "directory", str),
    ("ONEOPEN_PERSISTENCE_FSYNC", "persistence", "fsync", str),
    ("ONEOPEN_PERSISTENCE_SNAPSHOT_INTERVAL", "persistence", "snapshot_interval", int),
    ("ONEOPEN_QUEUES_VISIBILITY_TIMEOUT", "queues", "default_visibility_timeout", float),
    ("ONEOPEN_QUEUES_MAX_ATTEMPTS", "queues", "default_max_attempts", int),
    ("ONEOPEN_NETWORK_MAX_FRAME_SIZE", "network", "max_frame_size", int),
    ("ONEOPEN_NETWORK_MAX_CONNECTIONS", "network", "max_connections", int),
    ("ONEOPEN_PUBSUB_SUBSCRIBER_BUFFER", "pubsub", "subscriber_buffer", int),
    ("ONEOPEN_LOG_LEVEL", "logging", "level", str),
]


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name) or {}
    if not isinstance(value, dict):
        raise ValueError(f"config section {name!r} must be a mapping")
    return value


def _apply_mapping(cls: type, raw: dict[str, Any]) -> Any:
    valid = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
    kwargs = {k: v for k, v in raw.items() if k in valid}
    return cls(**kwargs)


def _load_security(raw: dict[str, Any]) -> SecurityConfig:
    tls = _apply_mapping(TlsConfig, raw.get("tls") or {})
    auth_raw = raw.get("auth") or {}
    users: list[AuthUser] = []
    for item in auth_raw.get("users") or []:
        if not isinstance(item, dict):
            raise ValueError("security.auth.users entries must be mappings")
        valid = {f.name for f in AuthUser.__dataclass_fields__.values()}
        users.append(AuthUser(**{k: v for k, v in item.items() if k in valid}))
    enabled = bool(auth_raw.get("enabled", False))
    return SecurityConfig(tls=tls, auth=AuthConfig(enabled=enabled, users=users))


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_config(
    path: str | Path | None = None,
    *,
    overrides: dict[str, Any] | None = None,
) -> BrokerConfig:
    data: dict[str, Any] = {}
    if path is not None:
        text = Path(path).read_text(encoding="utf-8")
        loaded = yaml.safe_load(text) or {}
        if not isinstance(loaded, dict):
            raise ValueError("config file must contain a YAML mapping")
        data = loaded

    cfg = BrokerConfig(
        server=_apply_mapping(ServerConfig, _section(data, "server")),
        persistence=_apply_mapping(PersistenceConfig, _section(data, "persistence")),
        queues=_apply_mapping(QueuesConfig, _section(data, "queues")),
        network=_apply_mapping(NetworkConfig, _section(data, "network")),
        pubsub=_apply_mapping(PubSubConfig, _section(data, "pubsub")),
        logging=_apply_mapping(LoggingConfig, _section(data, "logging")),
        security=_load_security(_section(data, "security")),
    )
    cfg = _apply_env(cfg)
    if overrides:
        cfg = _apply_overrides(cfg, overrides)
    return cfg


def _apply_env(cfg: BrokerConfig) -> BrokerConfig:
    sections = {
        "server": cfg.server,
        "persistence": cfg.persistence,
        "queues": cfg.queues,
        "network": cfg.network,
        "pubsub": cfg.pubsub,
        "logging": cfg.logging,
        "security": cfg.security,
    }
    for env_name, section, field_name, conv in _ENV_MAP:
        raw = os.environ.get(env_name)
        if raw is None or raw == "":
            continue
        current = sections[section]
        sections[section] = replace(current, **{field_name: conv(raw)})
    security = sections["security"]
    assert isinstance(security, SecurityConfig)
    tls = security.tls
    auth = security.auth
    tls_enabled = os.environ.get("ONEOPEN_TLS_ENABLED")
    if tls_enabled:
        tls = replace(tls, enabled=_truthy(tls_enabled))
    cert = os.environ.get("ONEOPEN_TLS_CERT")
    if cert:
        tls = replace(tls, certfile=cert, enabled=True)
    key = os.environ.get("ONEOPEN_TLS_KEY")
    if key:
        tls = replace(tls, keyfile=key)
    ca = os.environ.get("ONEOPEN_TLS_CA")
    if ca:
        tls = replace(tls, cafile=ca)
    auth_enabled = os.environ.get("ONEOPEN_AUTH_ENABLED")
    if auth_enabled:
        auth = replace(auth, enabled=_truthy(auth_enabled))
    token = os.environ.get("ONEOPEN_AUTH_TOKEN")
    if token:
        users = list(auth.users)
        users.append(AuthUser(name="env", token=token, role="admin"))
        auth = replace(auth, enabled=True, users=users)
    sections["security"] = SecurityConfig(tls=tls, auth=auth)
    return BrokerConfig(**sections)  # type: ignore[arg-type]


def _apply_overrides(cfg: BrokerConfig, overrides: dict[str, Any]) -> BrokerConfig:
    sections = {
        "server": cfg.server,
        "persistence": cfg.persistence,
        "queues": cfg.queues,
        "network": cfg.network,
        "pubsub": cfg.pubsub,
        "logging": cfg.logging,
        "security": cfg.security,
    }
    for key, value in overrides.items():
        if value is None:
            continue
        if not "." in key:
            continue
        parts = key.split(".", 2)
        section = parts[0]
        if section not in sections:
            continue
        if section == "security" and len(parts) >= 2:
            security = sections["security"]
            assert isinstance(security, SecurityConfig)
            if parts[1] == "tls":
                field_name = parts[2] if len(parts) == 3 else None
                if field_name:
                    sections["security"] = SecurityConfig(
                        tls=replace(security.tls, **{field_name: value}),
                        auth=security.auth,
                    )
            elif parts[1] == "auth":
                field_name = parts[2] if len(parts) == 3 else None
                if field_name == "enabled":
                    sections["security"] = SecurityConfig(
                        tls=security.tls,
                        auth=replace(security.auth, enabled=bool(value)),
                    )
                elif field_name == "users":
                    sections["security"] = SecurityConfig(
                        tls=security.tls,
                        auth=replace(security.auth, users=list(value), enabled=True),
                    )
            continue
        field_name = parts[1]
        sections[section] = replace(sections[section], **{field_name: value})
    return BrokerConfig(**sections)  # type: ignore[arg-type]
