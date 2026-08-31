from __future__ import annotations

from oneopen_broker.client.sync_client import Broker


def print_channels(client: Broker) -> None:
    rows = client.list_channels()
    print(f"{'CHANNEL':<24} {'SUBS':>8} {'PUBLISHED':>10} {'FANOUT':>10}")
    for row in rows:
        print(
            f"{row['channel']:<24} {row['subscribers']:>8} "
            f"{row['published']:>10} {row['fanout']:>10}"
        )
