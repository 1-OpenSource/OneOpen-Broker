"""Synchronous client wrapping AsyncBroker on a private event loop thread."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator
from typing import Any

from oneopen_broker.client.async_client import AsyncBroker, PubSubEvent, ReceivedMessage


class Broker:
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
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._async = AsyncBroker(
            host,
            port,
            token=token,
            username=username,
            password=password,
            ssl=ssl,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_insecure=ssl_insecure,
            server_hostname=server_hostname,
        )
        self._thread.start()
        self._call(self._async.connect())

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _call(self, coro, timeout: float = 30.0):
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=timeout)

    def close(self) -> None:
        try:
            self._call(self._async.close())
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=2)

    def __enter__(self) -> Broker:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def ping(self) -> bool:
        return self._call(self._async.ping())

    def publish(self, payload: bytes, **kwargs: Any) -> str:
        return self._call(self._async.publish(payload, **kwargs))

    def consume(self, queue: str, *, prefetch: int = 1) -> ReceivedMessage:
        return self._call(self._async.consume(queue, prefetch=prefetch))

    def get(self, queue: str) -> ReceivedMessage | None:
        return self._call(self._async.get(queue))

    def qos(self, prefetch: int, consumer_id: str | None = None) -> None:
        self._call(self._async.qos(prefetch, consumer_id))

    def ack(self, message_id: str) -> None:
        self._call(self._async.ack(message_id))

    def nack(self, message_id: str, requeue: bool = True) -> None:
        self._call(self._async.nack(message_id, requeue=requeue))

    def publish_channel(self, channel: str, payload: bytes) -> int:
        return self._call(self._async.publish_channel(channel, payload))

    def subscribe(self, channel: str) -> Iterator[PubSubEvent]:
        agen = self._async.subscribe(channel)

        async def _next() -> PubSubEvent:
            return await agen.__anext__()

        try:
            while True:
                yield self._call(_next())
        except StopAsyncIteration:
            return

    def stats(self) -> dict[str, Any]:
        return self._call(self._async.stats())

    def list_queues(self) -> list[dict[str, Any]]:
        return self._call(self._async.list_queues())

    def queue_info(self, name: str) -> dict[str, Any]:
        return self._call(self._async.queue_info(name))

    def list_consumers(self) -> list[dict[str, Any]]:
        return self._call(self._async.list_consumers())

    def list_channels(self) -> list[dict[str, Any]]:
        return self._call(self._async.list_channels())

    def dlq_info(self, queue: str) -> dict[str, Any]:
        return self._call(self._async.dlq_info(queue))

    def dlq_requeue(self, queue: str, message_id: str) -> None:
        self._call(self._async.dlq_requeue(queue, message_id))

    def declare_queue(self, name: str, **kwargs: Any) -> None:
        self._call(self._async.declare_queue(name, **kwargs))

    def declare_exchange(self, name: str, type: str = "direct") -> None:
        self._call(self._async.declare_exchange(name, type))

    def bind(self, exchange: str, queue: str, routing_key: str = "") -> None:
        self._call(self._async.bind(exchange, queue, routing_key))
