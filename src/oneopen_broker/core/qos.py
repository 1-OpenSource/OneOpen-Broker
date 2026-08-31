"""Consumer QoS / prefetch accounting."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class QoS:
    prefetch_count: int = 1
    unacked_count: int = 0

    def can_deliver(self) -> bool:
        if self.prefetch_count <= 0:
            return True
        return self.unacked_count < self.prefetch_count

    def on_deliver(self) -> bool:
        if not self.can_deliver():
            return False
        self.unacked_count += 1
        return True

    def on_ack(self) -> None:
        if self.unacked_count > 0:
            self.unacked_count -= 1

    def set_prefetch(self, count: int) -> None:
        self.prefetch_count = max(0, count)
