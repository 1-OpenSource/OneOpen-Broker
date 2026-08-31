"""Delayed-message heap helpers."""

from __future__ import annotations

import heapq
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass(slots=True)
class DelayedHeap:
    _heap: list[tuple[float, int, str, str]] = field(default_factory=list)

    def push(self, available_at: float, seq: int, queue: str, message_id: str) -> None:
        heapq.heappush(self._heap, (available_at, seq, queue, message_id))

    def pop_due(
        self,
        now: float,
        still_delayed: Callable[[str], bool],
    ) -> list[tuple[str, str]]:
        due: list[tuple[str, str]] = []
        heap = self._heap
        while heap and heap[0][0] <= now:
            _when, _seq, queue, message_id = heapq.heappop(heap)
            if still_delayed(message_id):
                due.append((queue, message_id))
        return due

    def __len__(self) -> int:
        return len(self._heap)
