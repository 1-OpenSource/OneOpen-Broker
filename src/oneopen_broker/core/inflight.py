from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class InflightRecord:
    message_id: str
    queue: str
    consumer_id: str
    delivery_time: float
    visibility_deadline: float
