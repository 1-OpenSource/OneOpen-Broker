from __future__ import annotations

import time
import uuid
from dataclasses import dataclass


@dataclass(slots=True)
class Message:
    id: str
    queue: str
    payload: bytes
    created_at: float
    attempts: int = 0
    max_attempts: int = 3
    priority: int = 0
    headers: dict | None = None
    available_at: float | None = None
    exchange: str = ""
    routing_key: str = ""

    @classmethod
    def create(
        cls,
        queue: str,
        payload: bytes,
        *,
        max_attempts: int = 3,
        priority: int = 0,
        headers: dict | None = None,
        available_at: float | None = None,
        exchange: str = "",
        routing_key: str = "",
        message_id: str | None = None,
        created_at: float | None = None,
        attempts: int = 0,
    ) -> Message:
        return cls(
            id=message_id or str(uuid.uuid4()),
            queue=queue,
            payload=payload,
            created_at=created_at if created_at is not None else time.time(),
            attempts=attempts,
            max_attempts=max_attempts,
            priority=priority,
            headers=headers,
            available_at=available_at,
            exchange=exchange,
            routing_key=routing_key,
        )
