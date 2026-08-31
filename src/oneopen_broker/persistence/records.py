"""Append-only file record codec."""

from __future__ import annotations

import json
import struct
import time
import zlib
from dataclasses import dataclass

MAGIC = b"OAOF"
VERSION = 1
HEADER_STRUCT = struct.Struct(">4sBHQdIII")
HEADER_SIZE = HEADER_STRUCT.size

OP_QUEUE_DECLARE = 1
OP_QUEUE_DELETE = 2
OP_EXCHANGE_DECLARE = 3
OP_EXCHANGE_DELETE = 4
OP_BIND = 5
OP_UNBIND = 6
OP_MESSAGE_PUBLISH = 7
OP_MESSAGE_ACK = 8
OP_MESSAGE_NACK = 9
OP_MESSAGE_REQUEUE = 10
OP_MESSAGE_DEAD = 11

OP_BY_NAME = {
    "QUEUE_DECLARE": OP_QUEUE_DECLARE,
    "QUEUE_DELETE": OP_QUEUE_DELETE,
    "EXCHANGE_DECLARE": OP_EXCHANGE_DECLARE,
    "EXCHANGE_DELETE": OP_EXCHANGE_DELETE,
    "BIND": OP_BIND,
    "UNBIND": OP_UNBIND,
    "MESSAGE_PUBLISH": OP_MESSAGE_PUBLISH,
    "MESSAGE_ACK": OP_MESSAGE_ACK,
    "MESSAGE_NACK": OP_MESSAGE_NACK,
    "MESSAGE_REQUEUE": OP_MESSAGE_REQUEUE,
    "MESSAGE_DEAD": OP_MESSAGE_DEAD,
}
NAME_BY_OP = {v: k for k, v in OP_BY_NAME.items()}


@dataclass(slots=True)
class AOFRecord:
    seq: int
    op: str
    meta: dict
    payload: bytes
    timestamp: float


def encode_record(seq: int, op: str, meta: dict, payload: bytes = b"", timestamp: float | None = None) -> bytes:
    op_id = OP_BY_NAME[op]
    ts = time.time() if timestamp is None else timestamp
    meta_b = json.dumps(meta, separators=(",", ":")).encode("utf-8")
    header_wo_crc = HEADER_STRUCT.pack(
        MAGIC, VERSION, op_id, seq, ts, len(meta_b), len(payload), 0
    )[: HEADER_SIZE - 4]
    crc = zlib.crc32(header_wo_crc + meta_b + payload) & 0xFFFFFFFF
    header = HEADER_STRUCT.pack(
        MAGIC, VERSION, op_id, seq, ts, len(meta_b), len(payload), crc
    )
    return header + meta_b + payload


def decode_record(buf: bytes) -> tuple[AOFRecord, int] | tuple[None, int]:
    """Return (record, bytes_consumed) or (None, 0) if incomplete.

    Raises ValueError on corrupt complete record. Partial tail returns (None, 0).
    """
    if len(buf) < HEADER_SIZE:
        return None, 0
    magic, version, op_id, seq, ts, meta_len, payload_len, crc = HEADER_STRUCT.unpack(
        buf[:HEADER_SIZE]
    )
    if magic != MAGIC or version != VERSION:
        raise ValueError("invalid AOF magic or version")
    total = HEADER_SIZE + meta_len + payload_len
    if len(buf) < total:
        return None, 0
    raw = buf[:total]
    meta_b = raw[HEADER_SIZE : HEADER_SIZE + meta_len]
    payload = raw[HEADER_SIZE + meta_len : total]
    header_wo_crc = raw[: HEADER_SIZE - 4]
    actual = zlib.crc32(header_wo_crc + meta_b + payload) & 0xFFFFFFFF
    if actual != crc:
        raise ValueError("AOF checksum mismatch")
    op = NAME_BY_OP.get(op_id)
    if op is None:
        raise ValueError(f"unknown AOF op {op_id}")
    meta = json.loads(meta_b.decode("utf-8")) if meta_b else {}
    return AOFRecord(seq=seq, op=op, meta=meta, payload=payload, timestamp=ts), total
