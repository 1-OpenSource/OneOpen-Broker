from __future__ import annotations

import pytest

from oneopen_broker.persistence.records import decode_record, encode_record
from oneopen_broker.persistence.snapshot import read_snapshot, write_snapshot


def test_aof_roundtrip() -> None:
    raw = encode_record(3, "MESSAGE_ACK", {"id": "abc", "queue": "t"})
    rec, n = decode_record(raw)
    assert rec is not None
    assert n == len(raw)
    assert rec.seq == 3
    assert rec.op == "MESSAGE_ACK"
    assert rec.meta["id"] == "abc"


def test_aof_incomplete() -> None:
    raw = encode_record(1, "QUEUE_DELETE", {"name": "t"})
    rec, n = decode_record(raw[:10])
    assert rec is None
    assert n == 0


def test_snapshot_roundtrip(tmp_path) -> None:
    path = tmp_path / "snapshot.dat"
    state = {
        "queues": {
            "seq": 4,
            "queues": [
                {
                    "spec": {
                        "name": "t",
                        "durable": True,
                        "max_length": None,
                        "visibility_timeout": 30,
                        "max_attempts": 3,
                        "dead_letter": True,
                    },
                    "messages": [
                        {
                            "id": "m1",
                            "queue": "t",
                            "payload": b"hello",
                            "created_at": 1.0,
                            "attempts": 0,
                            "max_attempts": 3,
                            "priority": 0,
                            "headers": None,
                            "available_at": None,
                            "exchange": "",
                            "routing_key": "",
                            "state": "READY",
                        }
                    ],
                    "stats": {},
                }
            ],
        },
        "routing": {"exchanges": [], "bindings": []},
    }
    write_snapshot(path, 9, state)
    loaded = read_snapshot(path)
    assert loaded is not None
    seq, restored = loaded
    assert seq == 9
    msg = restored["queues"]["queues"][0]["messages"][0]
    assert msg["payload"] == b"hello"
