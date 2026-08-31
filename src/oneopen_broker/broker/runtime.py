"""Composed in-memory broker state."""

from __future__ import annotations

from typing import Any

from oneopen_broker.broker.config import BrokerConfig
from oneopen_broker.core.consumers import ConsumerManager
from oneopen_broker.core.queue import Journal, NullJournal, QueueEngine
from oneopen_broker.core.routing import RoutingEngine
from oneopen_broker.monitoring.metrics import MetricsRegistry
from oneopen_broker.pubsub.channels import PubSubEngine


class BrokerRuntime:
    def __init__(self, config: BrokerConfig, journal: Journal | None = None) -> None:
        self.config = config
        self.journal: Journal = journal or NullJournal()
        q = config.queues
        self.queues = QueueEngine(
            visibility_timeout=q.default_visibility_timeout,
            max_attempts=q.default_max_attempts,
            max_length=q.default_max_length,
            retry_backoff=q.retry_backoff,
            retry_base_delay=q.retry_base_delay,
            recent_ack_limit=q.recent_ack_limit,
            journal=self.journal,
        )
        self.routing = RoutingEngine(self.queues, journal=self.journal)
        self.consumers = ConsumerManager(self.queues)
        self.pubsub = PubSubEngine(subscriber_buffer=config.pubsub.subscriber_buffer)
        self.metrics = MetricsRegistry()
        self.ready = False

    def snapshot(self) -> dict[str, Any]:
        return {
            "queues": self.queues.snapshot_state(),
            "routing": self.routing.snapshot_state(),
        }

    def restore_snapshot(self, state: dict[str, Any]) -> None:
        queues_state = state.get("queues") or {}
        if queues_state:
            self.queues.restore_state(queues_state)
        routing_state = state.get("routing") or {}
        if routing_state:
            self.routing.restore_state(routing_state)
