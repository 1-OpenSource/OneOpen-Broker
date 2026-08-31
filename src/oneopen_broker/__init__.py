"""OneOpen Broker — Python-native single-node message broker."""

from __future__ import annotations

from typing import Any

__version__ = "0.1.0"

__all__ = ["AsyncBroker", "Broker", "ReceivedMessage", "__version__"]


def __getattr__(name: str) -> Any:
    if name in {"AsyncBroker", "ReceivedMessage"}:
        from oneopen_broker.client.async_client import AsyncBroker, ReceivedMessage

        return {"AsyncBroker": AsyncBroker, "ReceivedMessage": ReceivedMessage}[name]
    if name == "Broker":
        from oneopen_broker.client.sync_client import Broker

        return Broker
    raise AttributeError(name)


def register_kombu() -> None:
    from oneopen_broker.kombu_transport import register_transport

    register_transport()
