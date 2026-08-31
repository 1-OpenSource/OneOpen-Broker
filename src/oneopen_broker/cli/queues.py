from __future__ import annotations

from oneopen_broker.client.sync_client import Broker


def print_queues(client: Broker) -> None:
    rows = client.list_queues()
    print(f"{'QUEUE':<24} {'READY':>8} {'INFLIGHT':>8} {'DELAYED':>8} {'DLQ':>8}")
    for row in rows:
        print(
            f"{row['name']:<24} {row['ready']:>8} {row['inflight']:>8} "
            f"{row['delayed']:>8} {row['dlq']:>8}"
        )


def print_queue(client: Broker, name: str) -> None:
    info = client.queue_info(name)
    for key, value in info.items():
        print(f"{key}: {value}")
