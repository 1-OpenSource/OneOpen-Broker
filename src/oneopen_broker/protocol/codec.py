"""JSON metadata codec on top of binary frames."""

from __future__ import annotations

import json
from typing import Any

from oneopen_broker.protocol.errors import INVALID_FRAME, ProtocolError
from oneopen_broker.protocol.frames import (
    FRAME_COMMAND,
    FRAME_EVENT,
    FRAME_HEARTBEAT,
    FRAME_RESPONSE,
    Frame,
    encode_frame,
)


def _dumps(meta: dict[str, Any]) -> bytes:
    return json.dumps(meta, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _loads(raw: bytes) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(INVALID_FRAME, "invalid metadata json") from exc
    if not isinstance(value, dict):
        raise ProtocolError(INVALID_FRAME, "metadata must be an object")
    return value


def command_frame(
    request_id: int,
    cmd: str,
    extra: dict[str, Any] | None = None,
    payload: bytes = b"",
) -> bytes:
    meta = {"cmd": cmd, **(extra or {})}
    return encode_frame(
        Frame(
            frame_type=FRAME_COMMAND,
            request_id=request_id,
            metadata=_dumps(meta),
            payload=payload,
        )
    )


def response_frame(
    request_id: int,
    ok: bool = True,
    extra: dict[str, Any] | None = None,
    payload: bytes = b"",
    code: str | None = None,
    message: str | None = None,
) -> bytes:
    meta: dict[str, Any] = {"ok": ok, **(extra or {})}
    if not ok:
        meta["code"] = code or "INTERNAL_ERROR"
        meta["message"] = message or meta["code"]
    return encode_frame(
        Frame(
            frame_type=FRAME_RESPONSE,
            request_id=request_id,
            metadata=_dumps(meta),
            payload=payload,
        )
    )


def event_frame(
    event: str,
    extra: dict[str, Any] | None = None,
    payload: bytes = b"",
    request_id: int = 0,
) -> bytes:
    meta = {"event": event, **(extra or {})}
    return encode_frame(
        Frame(
            frame_type=FRAME_EVENT,
            request_id=request_id,
            metadata=_dumps(meta),
            payload=payload,
        )
    )


def heartbeat_frame(request_id: int = 0) -> bytes:
    return encode_frame(
        Frame(
            frame_type=FRAME_HEARTBEAT,
            request_id=request_id,
            metadata=b"{}",
            payload=b"",
        )
    )


def parse_metadata(frame: Frame) -> dict[str, Any]:
    return _loads(frame.metadata)
