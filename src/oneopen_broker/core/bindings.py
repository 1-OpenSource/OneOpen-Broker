from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class Binding:
    exchange: str
    queue: str
    routing_key: str = ""
