"""Stable protocol error codes. Never send Python tracebacks to clients."""

from __future__ import annotations

OK = "OK"
INVALID_COMMAND = "INVALID_COMMAND"
INVALID_FRAME = "INVALID_FRAME"
NOT_FOUND = "NOT_FOUND"
QUEUE_FULL = "QUEUE_FULL"
ALREADY_EXISTS = "ALREADY_EXISTS"
NOT_BOUND = "NOT_BOUND"
NOT_CONSUMER = "NOT_CONSUMER"
UNKNOWN_DELIVERY = "UNKNOWN_DELIVERY"
PREFETCH_LIMIT = "PREFETCH_LIMIT"
ALREADY_ACKED = "ALREADY_ACKED"
SLOW_CONSUMER = "SLOW_CONSUMER"
FRAME_TOO_LARGE = "FRAME_TOO_LARGE"
UNAUTHORIZED = "UNAUTHORIZED"
FORBIDDEN = "FORBIDDEN"
INTERNAL_ERROR = "INTERNAL_ERROR"


class ProtocolError(Exception):
    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code
        self.message = message or code
