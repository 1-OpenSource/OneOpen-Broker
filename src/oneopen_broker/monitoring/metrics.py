"""In-memory metrics snapshot. Counters live on engines; this aggregates them."""

from __future__ import annotations

import os
import time
from typing import Any


class MetricsRegistry:
    def __init__(self) -> None:
        self.started_at = time.time()
        self.bytes_received = 0
        self.bytes_sent = 0
        self.connections = 0
        self.total_connections = 0

    def on_connect(self) -> None:
        self.connections += 1
        self.total_connections += 1

    def on_disconnect(self) -> None:
        if self.connections > 0:
            self.connections -= 1

    def add_received(self, n: int) -> None:
        self.bytes_received += n

    def add_sent(self, n: int) -> None:
        self.bytes_sent += n

    def uptime(self) -> float:
        return time.time() - self.started_at

    def broker_stats(self, runtime: Any) -> dict[str, Any]:
        published = delivered = acked = nacked = retried = dead = 0
        ready = inflight = delayed = dlq = 0
        for name in runtime.queues.list_queues():
            info = runtime.queues.queue_info(name) or {}
            published += info.get("published", 0)
            delivered += info.get("delivered", 0)
            acked += info.get("acked", 0)
            nacked += info.get("nacked", 0)
            retried += info.get("retried", 0)
            dead += info.get("dead", 0)
            ready += info.get("ready", 0)
            inflight += info.get("inflight", 0)
            delayed += info.get("delayed", 0)
            if name.endswith(".DLQ"):
                dlq += info.get("ready", 0)
        aof_size = 0
        snap_size = 0
        directory = runtime.config.persistence.directory
        aof_path = os.path.join(directory, "appendonly.aof")
        snap_path = os.path.join(directory, "snapshot.dat")
        if os.path.exists(aof_path):
            aof_size = os.path.getsize(aof_path)
        if os.path.exists(snap_path):
            snap_size = os.path.getsize(snap_path)
        return {
            "uptime": self.uptime(),
            "connections": self.connections,
            "total_connections": self.total_connections,
            "consumers": len(runtime.consumers.list_consumers()),
            "queues": len(runtime.queues.list_queues()),
            "channels": len(runtime.pubsub.list_channels()),
            "published": published,
            "delivered": delivered,
            "acked": acked,
            "nacked": nacked,
            "retried": retried,
            "dead": dead,
            "ready": ready,
            "inflight": inflight,
            "delayed": delayed,
            "dlq": dlq,
            "pubsub_published": runtime.pubsub.total_published,
            "pubsub_fanout": runtime.pubsub.total_fanout,
            "bytes_received": self.bytes_received,
            "bytes_sent": self.bytes_sent,
            "aof_size": aof_size,
            "snapshot_size": snap_size,
        }
