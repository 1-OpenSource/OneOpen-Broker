"""Async native client."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from oneopen_broker.client.connection import BrokerError, ProtocolClient


@dataclass(slots=True)
class ReceivedMessage:
    _client: AsyncBroker
    id: str
    queue: str
    payload: bytes
    attempts: int = 0
    headers: dict | None = None
    consumer_id: str = ""

    async def ack(self) -> None:
        await self._client.ack(self.id)

    async def nack(self, requeue: bool = True) -> None:
        await self._client.nack(self.id, requeue=requeue)


@dataclass(slots=True)
class PubSubEvent:
    channel: str
    payload: bytes


class AsyncBroker:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        *,
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        ssl: bool = False,
        ssl_cafile: str = "",
        ssl_certfile: str = "",
        ssl_keyfile: str = "",
        ssl_insecure: bool = False,
        server_hostname: str | None = None,
    ) -> None:
        self._token = token
        self._username = username
        self._password = password
        self._proto = ProtocolClient(
            host,
            port,
            ssl=ssl,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_insecure=ssl_insecure,
            server_hostname=server_hostname,
        )
        self._consumers: dict[str, str] = {}

    async def connect(self) -> None:
        await self._proto.connect()
        if self._token:
            await self._proto.request("AUTH", {"token": self._token})
        elif self._username:
            await self._proto.request(
                "AUTH",
                {"username": self._username, "password": self._password or ""},
            )

    async def close(self) -> None:
        await self._proto.close()

    async def __aenter__(self) -> AsyncBroker:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def ping(self) -> bool:
        meta, _ = await self._proto.request("PING")
        return bool(meta.get("pong"))

    async def publish(
        self,
        payload: bytes,
        *,
        queue: str | None = None,
        exchange: str | None = None,
        routing_key: str = "",
        headers: dict | None = None,
        priority: int = 0,
        max_attempts: int | None = None,
    ) -> str:
        extra: dict[str, Any] = {
            "queue": queue,
            "exchange": exchange,
            "routing_key": routing_key,
            "headers": headers,
            "priority": priority,
        }
        if max_attempts is not None:
            extra["max_attempts"] = max_attempts
        extra = {k: v for k, v in extra.items() if v is not None and v != ""}
        if queue:
            extra["queue"] = queue
        meta, _ = await self._proto.request("PUBLISH", extra, payload)
        return str(meta.get("id") or "")

    async def consume(self, queue: str, *, prefetch: int = 1) -> ReceivedMessage:
        if queue not in self._consumers:
            meta, _ = await self._proto.request(
                "CONSUME", {"queue": queue, "prefetch": prefetch}
            )
            self._consumers[queue] = str(meta["consumer_id"])
        meta, payload = await self._proto.deliveries.get()
        return ReceivedMessage(
            self,
            id=str(meta["message_id"]),
            queue=str(meta.get("queue") or queue),
            payload=payload,
            attempts=int(meta.get("attempts") or 0),
            headers=meta.get("headers"),
            consumer_id=str(meta.get("consumer_id") or ""),
        )

    async def get(self, queue: str) -> ReceivedMessage | None:
        meta, payload = await self._proto.request("GET", {"queue": queue})
        if meta.get("empty"):
            return None
        return ReceivedMessage(
            self,
            id=str(meta["message_id"]),
            queue=str(meta.get("queue") or queue),
            payload=payload,
            attempts=int(meta.get("attempts") or 0),
            headers=meta.get("headers"),
            consumer_id=str(meta.get("consumer_id") or ""),
        )

    async def qos(self, prefetch: int, consumer_id: str | None = None) -> None:
        extra: dict[str, Any] = {"prefetch": prefetch}
        if consumer_id:
            extra["consumer_id"] = consumer_id
        await self._proto.request("QOS", extra)

    async def ack(self, message_id: str) -> None:
        await self._proto.request("ACK", {"message_id": message_id})

    async def nack(self, message_id: str, requeue: bool = True) -> None:
        await self._proto.request("NACK", {"message_id": message_id, "requeue": requeue})

    async def subscribe(self, channel: str) -> AsyncIterator[PubSubEvent]:
        await self._proto.request("SUBSCRIBE", {"channel": channel})
        try:
            while True:
                meta, payload = await self._proto.pubsub.get()
                if meta.get("channel") == channel:
                    yield PubSubEvent(channel=channel, payload=payload)
        finally:
            try:
                await self._proto.request("UNSUBSCRIBE", {"channel": channel})
            except BrokerError:
                pass

    async def publish_channel(self, channel: str, payload: bytes) -> int:
        meta, _ = await self._proto.request(
            "PUBLISH_CHANNEL", {"channel": channel}, payload
        )
        return int(meta.get("delivered") or 0)

    async def stats(self) -> dict[str, Any]:
        meta, _ = await self._proto.request("STATS")
        return {k: v for k, v in meta.items() if k != "ok"}

    async def info(self) -> dict[str, Any]:
        meta, _ = await self._proto.request("INFO")
        return {k: v for k, v in meta.items() if k != "ok"}

    async def list_queues(self) -> list[dict[str, Any]]:
        meta, _ = await self._proto.request("LIST_QUEUES")
        return list(meta.get("queues") or [])

    async def queue_info(self, name: str) -> dict[str, Any]:
        meta, _ = await self._proto.request("QUEUE_INFO", {"name": name})
        return {k: v for k, v in meta.items() if k != "ok"}

    async def list_consumers(self) -> list[dict[str, Any]]:
        meta, _ = await self._proto.request("LIST_CONSUMERS")
        return list(meta.get("consumers") or [])

    async def list_channels(self) -> list[dict[str, Any]]:
        meta, _ = await self._proto.request("LIST_CHANNELS")
        return list(meta.get("channels") or [])

    async def dlq_info(self, queue: str) -> dict[str, Any]:
        meta, _ = await self._proto.request("DLQ_INFO", {"queue": queue})
        return {k: v for k, v in meta.items() if k != "ok"}

    async def dlq_requeue(self, queue: str, message_id: str) -> None:
        await self._proto.request("DLQ_REQUEUE", {"queue": queue, "message_id": message_id})

    async def declare_queue(self, name: str, **kwargs: Any) -> None:
        await self._proto.request("DECLARE_QUEUE", {"name": name, **kwargs})

    async def declare_exchange(self, name: str, type: str = "direct") -> None:
        await self._proto.request("DECLARE_EXCHANGE", {"name": name, "type": type})

    async def bind(self, exchange: str, queue: str, routing_key: str = "") -> None:
        await self._proto.request(
            "BIND", {"exchange": exchange, "queue": queue, "routing_key": routing_key}
        )
