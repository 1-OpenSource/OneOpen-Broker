"""Periodic broker snapshot. Payloads stored as length-prefixed blobs."""

from __future__ import annotations

import copy
import json
import logging
import os
import struct
import time
import zlib
from pathlib import Path
from typing import Any

log = logging.getLogger("oneopen_broker.persistence.snapshot")

MAGIC = b"OSNP"
VERSION = 1
HEADER = struct.Struct(">4sBdQII")  # magic, ver, created, aof_seq, meta_len, crc


def write_snapshot(path: str | Path, aof_seq: int, state: dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    state = copy.deepcopy(state)
    payloads: list[bytes] = []

    def _extract(messages: list[dict[str, Any]]) -> None:
        for msg in messages:
            payload = msg.get("payload", b"")
            if isinstance(payload, str):
                payload = payload.encode("utf-8")
            elif payload is None:
                payload = b""
            idx = len(payloads)
            payloads.append(payload)
            msg["payload"] = None
            msg["payload_index"] = idx

    queues_block = state.get("queues") or {}
    qlist = queues_block.get("queues") if isinstance(queues_block, dict) else queues_block
    if isinstance(qlist, list):
        for queue in qlist:
            if isinstance(queue, dict) and "messages" in queue:
                _extract(queue["messages"])

    meta = json.dumps({"aof_seq": aof_seq, "state": state}, separators=(",", ":")).encode("utf-8")
    blob = bytearray()
    for item in payloads:
        blob.extend(struct.pack(">I", len(item)))
        blob.extend(item)
    body = meta + bytes(blob)
    crc = zlib.crc32(body) & 0xFFFFFFFF
    created = time.time()
    # header: magic 4, ver 1, created 8, aof_seq 8, meta_len 4, blob_len 4, crc 4
    header = struct.pack(
        ">4sBdQII I".replace(" ", ""),
        MAGIC,
        VERSION,
        created,
        aof_seq,
        len(meta),
        len(blob),
        crc,
    )
    tmp = path.with_suffix(".dat.tmp")
    with open(tmp, "wb") as fp:
        fp.write(header)
        fp.write(meta)
        fp.write(blob)
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(tmp, path)
    log.info("wrote snapshot seq=%s path=%s", aof_seq, path)


def read_snapshot(path: str | Path) -> tuple[int, dict[str, Any]] | None:
    path = Path(path)
    if not path.exists():
        return None
    data = path.read_bytes()
    fmt = struct.Struct(">4sBdQII I".replace(" ", ""))
    if len(data) < fmt.size:
        log.warning("snapshot too small, ignoring")
        return None
    magic, version, _created, aof_seq, meta_len, blob_len, crc = fmt.unpack(data[: fmt.size])
    if magic != MAGIC or version != VERSION:
        log.warning("snapshot magic/version mismatch, ignoring")
        return None
    body = data[fmt.size : fmt.size + meta_len + blob_len]
    if len(body) != meta_len + blob_len:
        log.warning("truncated snapshot, ignoring")
        return None
    if (zlib.crc32(body) & 0xFFFFFFFF) != crc:
        log.warning("snapshot checksum mismatch, ignoring")
        return None
    meta = json.loads(body[:meta_len].decode("utf-8"))
    blob = body[meta_len:]
    payloads: list[bytes] = []
    offset = 0
    while offset + 4 <= len(blob):
        (n,) = struct.unpack_from(">I", blob, offset)
        offset += 4
        payloads.append(blob[offset : offset + n])
        offset += n
    state = meta["state"]
    queues = state.get("queues", {})
    qlist = queues.get("queues", queues if isinstance(queues, list) else [])
    if isinstance(qlist, list):
        for queue in qlist:
            for msg in queue.get("messages", []):
                idx = msg.get("payload_index")
                if idx is not None and 0 <= idx < len(payloads):
                    msg["payload"] = payloads[idx]
    return int(meta.get("aof_seq", aof_seq)), state
