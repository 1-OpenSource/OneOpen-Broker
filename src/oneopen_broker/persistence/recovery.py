"""Startup recovery: snapshot, then AOF replay, then inflight → READY."""

from __future__ import annotations

import logging
from pathlib import Path

from oneopen_broker.core.message import Message
from oneopen_broker.core.results import EnqueueResult
from oneopen_broker.persistence.records import AOFRecord, decode_record

log = logging.getLogger("oneopen_broker.persistence.recovery")


def read_aof(path: Path) -> tuple[list[AOFRecord], int]:
    """Return valid records and the file size to keep (truncate partial tail)."""
    if not path.exists():
        return [], 0
    data = path.read_bytes()
    records: list[AOFRecord] = []
    offset = 0
    while offset < len(data):
        try:
            rec, consumed = decode_record(data[offset:])
        except ValueError as exc:
            log.warning("AOF corrupt at offset %s: %s — truncating", offset, exc)
            break
        if rec is None:
            log.warning("AOF partial record at offset %s — truncating", offset)
            break
        records.append(rec)
        offset += consumed
    return records, offset


def apply_record(runtime, rec: AOFRecord) -> None:
    queues = runtime.queues
    routing = runtime.routing
    meta = rec.meta
    op = rec.op
    if op == "QUEUE_DECLARE":
        queues.declare_queue(
            meta["name"],
            durable=meta.get("durable", True),
            max_length=meta.get("max_length"),
            visibility_timeout=meta.get("visibility_timeout"),
            max_attempts=meta.get("max_attempts"),
            dead_letter=meta.get("dead_letter", True),
            persist=False,
        )
        routing.on_queue_declared(meta["name"], persist=False)
    elif op == "QUEUE_DELETE":
        queues.delete_queue(meta["name"], persist=False)
    elif op == "EXCHANGE_DECLARE":
        routing.declare_exchange(
            meta["name"],
            meta.get("type", "direct"),
            durable=meta.get("durable", True),
            persist=False,
        )
    elif op == "EXCHANGE_DELETE":
        routing.delete_exchange(meta["name"], persist=False)
    elif op == "BIND":
        if queues.get_queue(meta["queue"]) is None:
            queues.declare_queue(meta["queue"], persist=False)
        routing.bind(meta["exchange"], meta["queue"], meta.get("routing_key", ""), persist=False)
    elif op == "UNBIND":
        routing.unbind(meta["exchange"], meta["queue"], meta.get("routing_key", ""), persist=False)
    elif op == "MESSAGE_PUBLISH":
        if queues.get_queue(meta["queue"]) is None:
            queues.declare_queue(meta["queue"], persist=False)
            routing.on_queue_declared(meta["queue"], persist=False)
        message = Message(
            id=meta["id"],
            queue=meta["queue"],
            payload=rec.payload,
            created_at=meta.get("created_at", 0.0),
            attempts=meta.get("attempts", 0),
            max_attempts=meta.get("max_attempts", queues.max_attempts),
            priority=meta.get("priority", 0),
            headers=meta.get("headers"),
            available_at=meta.get("available_at"),
            exchange=meta.get("exchange", ""),
            routing_key=meta.get("routing_key", ""),
        )
        if message.id in queues._by_id:
            return
        result = queues.enqueue(message, persist=False)
        if result is EnqueueResult.QUEUE_FULL:
            log.warning("recovery skipped publish %s: queue full", meta["id"])
    elif op == "MESSAGE_ACK":
        queues.ack(meta["id"], persist=False)
    elif op == "MESSAGE_NACK":
        queues.nack(meta["id"], requeue=meta.get("requeue", True), persist=False)
    elif op == "MESSAGE_REQUEUE":
        if "from" in meta and "to" in meta:
            queues.dlq_requeue(meta["to"], meta["id"], persist=False)
        else:
            pass
    elif op == "MESSAGE_DEAD":
        pass


def recover_into(runtime, data_dir: str | Path) -> int:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    snap_path = data_dir / "snapshot.dat"
    aof_path = data_dir / "appendonly.aof"
    from oneopen_broker.persistence.snapshot import read_snapshot

    snap_seq = 0
    loaded = read_snapshot(snap_path)
    if loaded is not None:
        snap_seq, state = loaded
        runtime.restore_snapshot(state)
        log.info("loaded snapshot seq=%s", snap_seq)
    records, keep = read_aof(aof_path)
    if aof_path.exists():
        size = aof_path.stat().st_size
        if keep < size:
            with open(aof_path, "r+b") as fp:
                fp.truncate(keep)
            log.warning("truncated AOF from %s to %s bytes", size, keep)
    applied = 0
    last_seq = snap_seq
    for rec in records:
        if rec.seq <= snap_seq:
            continue
        apply_record(runtime, rec)
        applied += 1
        last_seq = rec.seq
    runtime.queues.tick()
    log.info("recovery complete applied=%s last_seq=%s", applied, last_seq)
    return last_seq
