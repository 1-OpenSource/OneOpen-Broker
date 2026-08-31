from __future__ import annotations

import enum


class AckResult(enum.Enum):
    OK = "OK"
    NOT_FOUND = "NOT_FOUND"
    ALREADY_ACKED = "ALREADY_ACKED"


class NackResult(enum.Enum):
    OK = "OK"
    NOT_FOUND = "NOT_FOUND"
    UNKNOWN_DELIVERY = "UNKNOWN_DELIVERY"


class EnqueueResult(enum.Enum):
    OK = "OK"
    QUEUE_FULL = "QUEUE_FULL"
    NOT_FOUND = "NOT_FOUND"
