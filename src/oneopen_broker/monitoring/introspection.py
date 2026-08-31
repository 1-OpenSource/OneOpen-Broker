"""Build introspection payloads from runtime state."""

from __future__ import annotations

from typing import Any


def list_queues(runtime: Any) -> list[dict[str, Any]]:
    return [
        runtime.queues.queue_info(name)
        for name in runtime.queues.list_queues()
        if runtime.queues.queue_info(name) is not None
    ]


def list_consumers(runtime: Any) -> list[dict[str, Any]]:
    now = runtime.queues.now()
    rows = []
    for consumer in runtime.consumers.list_consumers():
        rows.append(
            {
                "consumer_id": consumer.consumer_id,
                "connection_id": consumer.connection_id,
                "queues": consumer.queues,
                "prefetch": consumer.prefetch_count,
                "unacked": consumer.unacked_count,
                "age": now - consumer.connected_at,
                "last_activity": consumer.last_activity,
                "tag": consumer.tag,
            }
        )
    return rows
