from __future__ import annotations

from oneopen_broker.client.sync_client import Broker


def print_consumers(client: Broker) -> None:
    rows = client.list_consumers()
    print(f"{'CONSUMER':<36} {'QUEUE':<20} {'PREFETCH':>8} {'UNACKED':>8}")
    for row in rows:
        queues = ",".join(row.get("queues") or [])
        print(
            f"{row['consumer_id']:<36} {queues:<20} {row['prefetch']:>8} {row['unacked']:>8}"
        )
