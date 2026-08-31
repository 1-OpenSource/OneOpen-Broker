"""Dead-letter queue naming helpers."""

from __future__ import annotations

DLQ_SUFFIX = ".DLQ"


def dlq_name(queue: str) -> str:
    if queue.endswith(DLQ_SUFFIX):
        return queue
    return f"{queue}{DLQ_SUFFIX}"


def is_dlq(queue: str) -> bool:
    return queue.endswith(DLQ_SUFFIX)


def origin_queue(dlq: str) -> str:
    if dlq.endswith(DLQ_SUFFIX):
        return dlq[: -len(DLQ_SUFFIX)]
    return dlq
