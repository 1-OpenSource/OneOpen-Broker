"""Direct and fanout routing."""

from __future__ import annotations

from typing import Any

from oneopen_broker.core.bindings import Binding
from oneopen_broker.core.exchange import Exchange
from oneopen_broker.core.message import Message
from oneopen_broker.core.queue import Journal, NullJournal, QueueEngine
from oneopen_broker.core.results import EnqueueResult

DEFAULT_EXCHANGE = ""


class RoutingEngine:
    def __init__(self, queues: QueueEngine, journal: Journal | None = None) -> None:
        self.queues = queues
        self.journal: Journal = journal or NullJournal()
        self._exchanges: dict[str, Exchange] = {}
        self._bindings: set[Binding] = set()
        self.declare_exchange(DEFAULT_EXCHANGE, "direct", persist=False)

    def declare_exchange(
        self,
        name: str,
        type: str = "direct",
        *,
        durable: bool = True,
        persist: bool = True,
    ) -> Exchange:
        existing = self._exchanges.get(name)
        if existing is not None:
            return existing
        exchange = Exchange(name=name, type=type, durable=durable)
        self._exchanges[name] = exchange
        if persist:
            self.journal.append(
                "EXCHANGE_DECLARE",
                {"name": name, "type": type, "durable": durable},
            )
        return exchange

    def delete_exchange(self, name: str, *, persist: bool = True) -> bool:
        if name == DEFAULT_EXCHANGE:
            return False
        if name not in self._exchanges:
            return False
        self._exchanges.pop(name)
        self._bindings = {b for b in self._bindings if b.exchange != name}
        if persist:
            self.journal.append("EXCHANGE_DELETE", {"name": name})
        return True

    def bind(
        self,
        exchange: str,
        queue: str,
        routing_key: str = "",
        *,
        persist: bool = True,
    ) -> bool:
        if exchange not in self._exchanges:
            return False
        if self.queues.get_queue(queue) is None:
            return False
        binding = Binding(exchange=exchange, queue=queue, routing_key=routing_key)
        self._bindings.add(binding)
        if persist:
            self.journal.append(
                "BIND",
                {"exchange": exchange, "queue": queue, "routing_key": routing_key},
            )
        return True

    def unbind(
        self,
        exchange: str,
        queue: str,
        routing_key: str = "",
        *,
        persist: bool = True,
    ) -> bool:
        binding = Binding(exchange=exchange, queue=queue, routing_key=routing_key)
        if binding not in self._bindings:
            return False
        self._bindings.discard(binding)
        if persist:
            self.journal.append(
                "UNBIND",
                {"exchange": exchange, "queue": queue, "routing_key": routing_key},
            )
        return True

    def on_queue_declared(self, queue: str, *, persist: bool = True) -> None:
        self.bind(DEFAULT_EXCHANGE, queue, queue, persist=persist)

    def route_targets(self, exchange: str, routing_key: str) -> list[str]:
        ex = self._exchanges.get(exchange)
        if ex is None:
            return []
        if ex.type == "fanout":
            return sorted({b.queue for b in self._bindings if b.exchange == exchange})
        return sorted(
            {
                b.queue
                for b in self._bindings
                if b.exchange == exchange and b.routing_key == routing_key
            }
        )

    def publish(
        self,
        payload: bytes,
        *,
        exchange: str | None = None,
        routing_key: str = "",
        queue: str | None = None,
        max_attempts: int | None = None,
        priority: int = 0,
        headers: dict | None = None,
        message_id: str | None = None,
        persist: bool = True,
    ) -> tuple[str, list[str], EnqueueResult]:
        if queue and not exchange:
            exchange = DEFAULT_EXCHANGE
            routing_key = routing_key or queue
            if self.queues.get_queue(queue) is None:
                self.queues.declare_queue(queue, persist=persist)
                self.on_queue_declared(queue, persist=persist)
        exchange = DEFAULT_EXCHANGE if exchange is None else exchange
        if exchange not in self._exchanges:
            return "", [], EnqueueResult.NOT_FOUND
        targets = self.route_targets(exchange, routing_key)
        if not targets:
            return "", [], EnqueueResult.NOT_FOUND
        routed: list[str] = []
        last_id = ""
        for target in targets:
            message = Message.create(
                target,
                payload,
                max_attempts=max_attempts or self.queues.max_attempts,
                priority=priority,
                headers=headers,
                exchange=exchange,
                routing_key=routing_key,
                message_id=message_id if len(targets) == 1 else None,
            )
            result = self.queues.enqueue(message, persist=persist)
            if result is EnqueueResult.QUEUE_FULL:
                return message.id, routed, result
            if result is EnqueueResult.OK:
                routed.append(target)
                last_id = message.id
        if not routed:
            return last_id, routed, EnqueueResult.NOT_FOUND
        return last_id, routed, EnqueueResult.OK

    def snapshot_state(self) -> dict[str, Any]:
        return {
            "exchanges": [
                {"name": e.name, "type": e.type, "durable": e.durable}
                for e in self._exchanges.values()
            ],
            "bindings": [
                {"exchange": b.exchange, "queue": b.queue, "routing_key": b.routing_key}
                for b in self._bindings
            ],
        }

    def restore_state(self, state: dict[str, Any]) -> None:
        self._exchanges.clear()
        self._bindings.clear()
        self.declare_exchange(DEFAULT_EXCHANGE, "direct", persist=False)
        for raw in state.get("exchanges", []):
            self.declare_exchange(
                raw["name"],
                raw.get("type", "direct"),
                durable=raw.get("durable", True),
                persist=False,
            )
        for raw in state.get("bindings", []):
            self.bind(
                raw["exchange"],
                raw["queue"],
                raw.get("routing_key", ""),
                persist=False,
            )
