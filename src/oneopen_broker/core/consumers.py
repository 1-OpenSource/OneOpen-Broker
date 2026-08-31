"""Connected consumer registry and fair delivery."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from oneopen_broker.core.message import Message
from oneopen_broker.core.qos import QoS
from oneopen_broker.core.queue import QueueEngine


@dataclass(slots=True)
class Consumer:
    consumer_id: str
    connection_id: str
    queues: list[str]
    qos: QoS = field(default_factory=QoS)
    connected_at: float = 0.0
    last_activity: float = 0.0
    tag: str = ""
    cancelled: bool = False

    @property
    def prefetch_count(self) -> int:
        return self.qos.prefetch_count

    @property
    def unacked_count(self) -> int:
        return self.qos.unacked_count


class ConsumerManager:
    def __init__(
        self,
        engine: QueueEngine,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.engine = engine
        self._now = clock or time.time
        self._consumers: dict[str, Consumer] = {}
        self._by_connection: dict[str, set[str]] = {}

    def register(
        self,
        consumer_id: str,
        connection_id: str,
        queues: list[str],
        *,
        prefetch_count: int = 1,
        tag: str = "",
    ) -> Consumer:
        now = self._now()
        consumer = Consumer(
            consumer_id=consumer_id,
            connection_id=connection_id,
            queues=list(queues),
            qos=QoS(prefetch_count=prefetch_count),
            connected_at=now,
            last_activity=now,
            tag=tag,
        )
        self._consumers[consumer_id] = consumer
        self._by_connection.setdefault(connection_id, set()).add(consumer_id)
        for name in queues:
            queue = self.engine.declare_queue(name)
            if consumer_id not in queue.consumer_ids:
                queue.consumer_ids.append(consumer_id)
        return consumer

    def qos(self, consumer_id: str, prefetch_count: int) -> bool:
        consumer = self._consumers.get(consumer_id)
        if consumer is None:
            return False
        consumer.qos.set_prefetch(prefetch_count)
        consumer.last_activity = self._now()
        return True

    def cancel(self, consumer_id: str, *, recover: bool = True) -> bool:
        consumer = self._consumers.pop(consumer_id, None)
        if consumer is None:
            return False
        consumer.cancelled = True
        conns = self._by_connection.get(consumer.connection_id)
        if conns is not None:
            conns.discard(consumer_id)
            if not conns:
                self._by_connection.pop(consumer.connection_id, None)
        for name in consumer.queues:
            queue = self.engine.get_queue(name)
            if queue is not None and consumer_id in queue.consumer_ids:
                queue.consumer_ids.remove(consumer_id)
        if recover:
            self.engine.recover_consumer(consumer_id)
        return True

    def disconnect_connection(self, connection_id: str) -> int:
        ids = list(self._by_connection.get(connection_id, ()))
        n = 0
        for consumer_id in ids:
            if self.cancel(consumer_id, recover=True):
                n += 1
        return n

    def get(self, consumer_id: str) -> Consumer | None:
        return self._consumers.get(consumer_id)

    def list_consumers(self) -> list[Consumer]:
        return list(self._consumers.values())

    def on_ack(self, consumer_id: str) -> None:
        consumer = self._consumers.get(consumer_id)
        if consumer is not None:
            consumer.qos.on_ack()
            consumer.last_activity = self._now()

    def try_reserve(self, queue_name: str) -> tuple[Consumer, Message] | None:
        queue = self.engine.get_queue(queue_name)
        if queue is None or not queue.consumer_ids:
            return None
        n = len(queue.consumer_ids)
        for _ in range(n):
            idx = queue.rr_index % n
            queue.rr_index += 1
            consumer_id = queue.consumer_ids[idx]
            consumer = self._consumers.get(consumer_id)
            if consumer is None or consumer.cancelled:
                continue
            if not consumer.qos.can_deliver():
                continue
            message = self.engine.reserve(queue_name, consumer_id)
            if message is None:
                return None
            consumer.qos.on_deliver()
            consumer.last_activity = self._now()
            return consumer, message
        return None

    def drain_queue(self, queue_name: str) -> list[tuple[Consumer, Message]]:
        delivered: list[tuple[Consumer, Message]] = []
        while True:
            item = self.try_reserve(queue_name)
            if item is None:
                break
            delivered.append(item)
        return delivered
