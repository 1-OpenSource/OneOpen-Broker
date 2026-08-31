from __future__ import annotations

from oneopen_broker.core.queue import QueueEngine
from oneopen_broker.core.results import AckResult
from oneopen_broker.protocol.codec import command_frame
from oneopen_broker.protocol.errors import ProtocolError
from oneopen_broker.protocol.frames import FrameReader


def test_malformed_magic_does_not_raise_beyond_protocol_error() -> None:
    reader = FrameReader(max_frame_size=1_000_000)
    try:
        reader.feed(b"XXXX" + b"\x00" * 40)
    except ProtocolError:
        pass
    else:
        raise AssertionError("expected ProtocolError")


def test_duplicate_ack_state() -> None:
    eng = QueueEngine()
    eng.declare_queue("t")
    from oneopen_broker.core.message import Message

    eng.enqueue(Message.create("t", b"x"))
    msg = eng.reserve("t", "c")
    assert msg is not None
    assert eng.ack(msg.id) is AckResult.OK
    assert eng.ack(msg.id) is AckResult.ALREADY_ACKED


def test_truncated_frame() -> None:
    raw = command_frame(1, "PING")
    reader = FrameReader(max_frame_size=1_000_000)
    assert reader.feed(raw[:8]) == []
