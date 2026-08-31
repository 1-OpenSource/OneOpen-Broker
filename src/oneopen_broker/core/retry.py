"""Retry delay calculation. Delayed messages live on the engine deadline heap."""

from __future__ import annotations


def retry_delay(attempts: int, *, mode: str = "exponential", base: float = 1.0) -> float:
    if attempts < 1:
        attempts = 1
    if mode == "fixed":
        return float(base)
    return float(base) * (2 ** (attempts - 1))
