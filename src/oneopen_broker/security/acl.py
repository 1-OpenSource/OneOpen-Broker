"""Command-level roles. V1 ACLs are role-based, not per-queue."""

from __future__ import annotations

from oneopen_broker.protocol.errors import FORBIDDEN, UNAUTHORIZED
from oneopen_broker.security.auth import Principal

UNAUTHENTICATED_COMMANDS = frozenset({"PING", "AUTH"})

_PRODUCER = frozenset(
    {
        "PING",
        "AUTH",
        "PUBLISH",
        "DECLARE_QUEUE",
        "DECLARE_EXCHANGE",
        "BIND",
        "UNBIND",
        "QUEUE_INFO",
        "LIST_QUEUES",
    }
)
_CONSUMER = frozenset(
    {
        "PING",
        "AUTH",
        "CONSUME",
        "GET",
        "ACK",
        "NACK",
        "CANCEL",
        "QOS",
        "DECLARE_QUEUE",
        "QUEUE_INFO",
    }
)
_SUBSCRIBER = frozenset(
    {
        "PING",
        "AUTH",
        "SUBSCRIBE",
        "UNSUBSCRIBE",
        "PUBLISH_CHANNEL",
        "LIST_CHANNELS",
    }
)
_MONITOR = frozenset(
    {
        "PING",
        "AUTH",
        "INFO",
        "STATS",
        "LIST_QUEUES",
        "QUEUE_INFO",
        "LIST_CONSUMERS",
        "LIST_CHANNELS",
        "DLQ_INFO",
    }
)

ROLE_COMMANDS: dict[str, frozenset[str] | None] = {
    "admin": None,
    "producer": _PRODUCER,
    "consumer": _CONSUMER,
    "subscriber": _SUBSCRIBER,
    "monitor": _MONITOR,
}


def authorize(principal: Principal | None, command: str, *, auth_required: bool) -> tuple[bool, str, str]:
    """Return (allowed, error_code, message)."""
    cmd = command.upper()
    if cmd in UNAUTHENTICATED_COMMANDS:
        return True, "", ""
    if auth_required and principal is None:
        return False, UNAUTHORIZED, "authentication required"
    if principal is None:
        return True, "", ""
    allowed = ROLE_COMMANDS.get(principal.role)
    if allowed is None:
        return True, "", ""
    if cmd in allowed:
        return True, "", ""
    return False, FORBIDDEN, f"role {principal.role} cannot {cmd}"
