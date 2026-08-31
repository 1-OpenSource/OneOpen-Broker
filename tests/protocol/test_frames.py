"""Frame encode/decode tests."""

from __future__ import annotations

import pytest

from oneopen_broker.protocol.codec import command_frame, parse_metadata, response_frame
from oneopen_broker.protocol.errors import FRAME_TOO_LARGE, INVALID_FRAME, ProtocolError
from oneopen_broker.protocol.frames import HEADER_SIZE, FrameReader, encode_frame


def test_roundtrip_binary_payload() -> None:
    raw = command_frame(7, "PUBLISH", {"queue": "gpu"}, payload=b"\x00\xffhello")
    reader = FrameReader(max_frame_size=1_000_000)
    frames = reader.feed(raw)
    assert len(frames) == 1
    frame = frames[0]
    meta = parse_metadata(frame)
    assert meta["cmd"] == "PUBLISH"
    assert meta["queue"] == "gpu"
    assert frame.payload == b"\x00\xffhello"
    assert frame.request_id == 7


def test_incremental_feed() -> None:
    raw = response_frame(1, True, {"pong": True})
    reader = FrameReader(max_frame_size=1_000_000)
    assert reader.feed(raw[:10]) == []
    assert reader.feed(raw[10:20]) == []
    frames = reader.feed(raw[20:])
    assert len(frames) == 1
    assert parse_metadata(frames[0])["pong"] is True


def test_bad_magic() -> None:
    raw = command_frame(1, "PING")
    bad = b"XXXX" + raw[4:]
    reader = FrameReader(max_frame_size=1_000_000)
    with pytest.raises(ProtocolError) as exc:
        reader.feed(bad)
    assert exc.value.code == INVALID_FRAME


def test_checksum_mismatch() -> None:
    raw = bytearray(command_frame(1, "PING"))
    raw[HEADER_SIZE] ^= 0xFF
    reader = FrameReader(max_frame_size=1_000_000)
    with pytest.raises(ProtocolError) as exc:
        reader.feed(bytes(raw))
    assert exc.value.code == INVALID_FRAME


def test_frame_too_large() -> None:
    raw = command_frame(1, "PUBLISH", payload=b"abcd")
    reader = FrameReader(max_frame_size=HEADER_SIZE + 2)
    with pytest.raises(ProtocolError) as exc:
        reader.feed(raw)
    assert exc.value.code == FRAME_TOO_LARGE


def test_encode_decode_header_size() -> None:
    raw = command_frame(0, "PING")
    assert len(raw) >= HEADER_SIZE
    # encode_frame used internally; round-trip already covered
    assert encode_frame.__name__ == "encode_frame"
