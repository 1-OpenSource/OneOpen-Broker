"""Ephemeral Pub/Sub. No history, offsets, or replay."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field


SendFn = Callable[[bytes], bool]


@dataclass(slots=True)
class ChannelStats:
    published: int = 0
    fanout: int = 0
    slow_drops: int = 0


@dataclass(slots=True)
class PubSubEngine:
    subscriber_buffer: int = 1000
    _subs: dict[str, dict[str, SendFn]] = field(default_factory=lambda: defaultdict(dict))
    _by_connection: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    _stats: dict[str, ChannelStats] = field(default_factory=lambda: defaultdict(ChannelStats))
    total_published: int = 0
    total_fanout: int = 0

    def subscribe(self, connection_id: str, channel: str, send: SendFn) -> None:
        self._subs[channel][connection_id] = send
        self._by_connection[connection_id].add(channel)

    def unsubscribe(self, connection_id: str, channel: str) -> bool:
        conns = self._subs.get(channel)
        if not conns or connection_id not in conns:
            return False
        conns.pop(connection_id, None)
        self._by_connection[connection_id].discard(channel)
        if not conns:
            self._subs.pop(channel, None)
        return True

    def disconnect(self, connection_id: str) -> None:
        channels = list(self._by_connection.pop(connection_id, ()))
        for channel in channels:
            self.unsubscribe(connection_id, channel)

    def publish(self, channel: str, payload: bytes, send_event: Callable[[str, bytes], bool]) -> dict:
        self.total_published += 1
        stats = self._stats[channel]
        stats.published += 1
        delivered = 0
        slow: list[str] = []
        for connection_id in list(self._subs.get(channel, {})):
            ok = send_event(connection_id, payload)
            if ok:
                delivered += 1
                self.total_fanout += 1
                stats.fanout += 1
            else:
                slow.append(connection_id)
                stats.slow_drops += 1
        return {"delivered": delivered, "slow": slow}

    def list_channels(self) -> list[dict]:
        names = set(self._subs) | set(self._stats)
        result = []
        for name in sorted(names):
            result.append(
                {
                    "channel": name,
                    "subscribers": len(self._subs.get(name, {})),
                    "published": self._stats[name].published,
                    "fanout": self._stats[name].fanout,
                }
            )
        return result
