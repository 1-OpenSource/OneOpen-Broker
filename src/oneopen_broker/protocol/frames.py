from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass

from oneopen_broker.protocol.errors import FRAME_TOO_LARGE, INVALID_FRAME, ProtocolError

MAGIC = b"OOB1"
VERSION = 1
HEADER_SIZE = 28
HEADER_STRUCT = struct.Struct(">4sBBHQII I".replace(" ", ""))

FRAME_COMMAND = 0x01
FRAME_RESPONSE = 0x02
FRAME_EVENT = 0x03
FRAME_HEARTBEAT = 0x04

assert HEADER_STRUCT.size == HEADER_SIZE


@dataclass(slots=True)
class Frame:
    frame_type: int
    request_id: int
    metadata: bytes
    payload: bytes
    flags: int = 0
    version: int = VERSION

    @property
    def is_command(self) -> bool:
        return self.frame_type == FRAME_COMMAND

    @property
    def is_response(self) -> bool:
        return self.frame_type == FRAME_RESPONSE

    @property
    def is_event(self) -> bool:
        return self.frame_type == FRAME_EVENT


def _crc(header24: bytes, metadata: bytes, payload: bytes) -> int:
    return zlib.crc32(header24 + metadata + payload) & 0xFFFFFFFF


def encode_frame(frame: Frame) -> bytes:
    meta = frame.metadata
    payload = frame.payload
    header24 = HEADER_STRUCT.pack(
        MAGIC,
        frame.version,
        frame.frame_type,
        frame.flags,
        frame.request_id,
        len(meta),
        len(payload),
        0,
    )[:24]
    crc = _crc(header24, meta, payload)
    header = HEADER_STRUCT.pack(
        MAGIC,
        frame.version,
        frame.frame_type,
        frame.flags,
        frame.request_id,
        len(meta),
        len(payload),
        crc,
    )
    return header + meta + payload


def decode_header(header: bytes) -> tuple[int, int, int, int, int, int, int]:
    if len(header) < HEADER_SIZE:
        raise ProtocolError(INVALID_FRAME, "truncated header")
    magic, version, frame_type, flags, request_id, meta_len, payload_len, crc = (
        HEADER_STRUCT.unpack(header[:HEADER_SIZE])
    )
    if magic != MAGIC:
        raise ProtocolError(INVALID_FRAME, "bad magic")
    if version != VERSION:
        raise ProtocolError(INVALID_FRAME, "unsupported version")
    return frame_type, flags, request_id, meta_len, payload_len, crc, version


class FrameReader:
    def __init__(self, max_frame_size: int) -> None:
        self.max_frame_size = max_frame_size
        self._buf = bytearray()
        self._closed_error: ProtocolError | None = None

    def feed(self, data: bytes) -> list[Frame]:
        if self._closed_error is not None:
            raise self._closed_error
        self._buf.extend(data)
        frames: list[Frame] = []
        while True:
            frame = self._try_one()
            if frame is None:
                break
            frames.append(frame)
        return frames

    def _try_one(self) -> Frame | None:
        buf = self._buf
        if len(buf) < HEADER_SIZE:
            return None
        try:
            frame_type, flags, request_id, meta_len, payload_len, crc, version = decode_header(
                bytes(buf[:HEADER_SIZE])
            )
        except ProtocolError as exc:
            self._closed_error = exc
            raise
        total = HEADER_SIZE + meta_len + payload_len
        if total > self.max_frame_size or meta_len < 0 or payload_len < 0:
            err = ProtocolError(FRAME_TOO_LARGE, "frame exceeds max_frame_size")
            self._closed_error = err
            raise err
        if len(buf) < total:
            return None
        raw = bytes(buf[:total])
        del buf[:total]
        metadata = raw[HEADER_SIZE : HEADER_SIZE + meta_len]
        payload = raw[HEADER_SIZE + meta_len : total]
        header24 = raw[:24]
        actual = _crc(header24, metadata, payload)
        if actual != crc:
            err = ProtocolError(INVALID_FRAME, "checksum mismatch")
            self._closed_error = err
            raise err
        return Frame(
            frame_type=frame_type,
            request_id=request_id,
            metadata=metadata,
            payload=payload,
            flags=flags,
            version=version,
        )
