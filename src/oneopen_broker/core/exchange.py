from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Exchange:
    name: str
    type: str = "direct"
    durable: bool = True

    def __post_init__(self) -> None:
        if self.type not in {"direct", "fanout"}:
            raise ValueError("V1 supports only direct and fanout exchanges")
