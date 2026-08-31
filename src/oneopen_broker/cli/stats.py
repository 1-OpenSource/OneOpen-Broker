from __future__ import annotations

from oneopen_broker.client.sync_client import Broker


def _fmt(n: object) -> str:
    if isinstance(n, float):
        return f"{n:,.1f}"
    if isinstance(n, int):
        return f"{n:,}"
    return str(n)


def print_stats(client: Broker) -> None:
    stats = client.stats()
    width = max((len(k) for k in stats), default=8)
    for key, value in stats.items():
        print(f"{key:<{width}}  {_fmt(value)}")
