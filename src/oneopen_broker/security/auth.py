"""Authentication principals and token/password verification."""

from __future__ import annotations

import hmac
import logging
from dataclasses import dataclass

log = logging.getLogger("oneopen_broker.security.auth")

ROLES = ("admin", "producer", "consumer", "subscriber", "monitor")


@dataclass(slots=True)
class Principal:
    name: str
    role: str

    def __post_init__(self) -> None:
        if self.role not in ROLES:
            raise ValueError(f"unknown role {self.role!r}")


def _digest(left: str, right: str) -> bool:
    a = left.encode("utf-8")
    b = right.encode("utf-8")
    if len(a) != len(b):
        hmac.compare_digest(b, b)
        return False
    return hmac.compare_digest(a, b)


def authenticate(
    *,
    token: str | None,
    username: str | None,
    password: str | None,
    users: list,
) -> Principal | None:
    """Return a principal on success. Never logs secrets."""
    if token:
        for user in users:
            secret = getattr(user, "token", "") or ""
            if secret and _digest(token, secret):
                return Principal(name=user.name, role=user.role)
        log.warning("authentication failed (token)")
        return None
    if username and password:
        for user in users:
            if user.name != username:
                continue
            secret = getattr(user, "password", "") or getattr(user, "token", "") or ""
            if secret and _digest(password, secret):
                return Principal(name=user.name, role=user.role)
        log.warning("authentication failed user=%s", username)
        return None
    log.warning("authentication failed (missing credentials)")
    return None
