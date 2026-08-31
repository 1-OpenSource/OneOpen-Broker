from __future__ import annotations

import time

from oneopen_broker import __version__
from oneopen_broker.client.sync_client import Broker


def _uptime(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _screen(stats: dict, queues: list[dict], prev: dict | None, interval: float) -> str:
    def rate(key: str) -> str:
        if not prev:
            return "0"
        delta = float(stats.get(key, 0) - prev.get(key, 0))
        return f"{delta / interval:,.0f}"

    lines = [
        f"OneOpen Broker {__version__}",
        f"Uptime: {_uptime(float(stats.get('uptime') or 0))}",
        "",
        f"Connections:  {stats.get('connections', 0):>6}",
        f"Consumers:    {stats.get('consumers', 0):>6}",
        f"Queues:       {stats.get('queues', 0):>6}",
        f"Channels:     {stats.get('channels', 0):>6}",
        "",
        "Rate",
        "─" * 28,
        f"Published/s      {rate('published'):>10}",
        f"Delivered/s      {rate('delivered'):>10}",
        f"ACK/s            {rate('acked'):>10}",
        f"NACK/s           {rate('nacked'):>10}",
        "",
        "Queues",
        "─" * 64,
        f"{'QUEUE':<18} {'READY':>8} {'INFLIGHT':>10} {'DELAYED':>9} {'DLQ':>6}",
    ]
    for row in queues:
        if str(row.get("name", "")).endswith(".DLQ"):
            continue
        lines.append(
            f"{row['name']:<18} {row['ready']:>8} {row['inflight']:>10} "
            f"{row['delayed']:>9} {row['dlq']:>6}"
        )
    return "\n".join(lines)


def run_top(client: Broker, interval: float = 1.0) -> None:
    prev = None
    try:
        while True:
            stats = client.stats()
            queues = client.list_queues()
            text = _screen(stats, queues, prev, interval)
            print("\033[2J\033[H" + text, flush=True)
            prev = stats
            time.sleep(interval)
    except KeyboardInterrupt:
        print()
