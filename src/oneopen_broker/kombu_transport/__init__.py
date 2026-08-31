"""Kombu transport registration for oneopen:// URLs."""

from __future__ import annotations


def register_transport() -> None:
    try:
        from kombu.transport import TRANSPORT_ALIASES
    except ImportError:
        return
    TRANSPORT_ALIASES["oneopen"] = "oneopen_broker.kombu_transport.transport:Transport"


__all__ = ["register_transport"]
