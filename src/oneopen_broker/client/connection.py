"""Low-level async protocol client used by the SDK and CLI."""

from __future__ import annotations

import asyncio
from typing import Any

from oneopen_broker.protocol.codec import command_frame, heartbeat_frame, parse_metadata
from oneopen_broker.protocol.errors import ProtocolError
from oneopen_broker.protocol.frames import (
    FRAME_EVENT,
    FRAME_HEARTBEAT,
    FRAME_RESPONSE,
    FrameReader,
)
from oneopen_broker.security.tls import client_ssl_context


class BrokerError(Exception):
    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code
        self.message = message or code


class ProtocolClient:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        *,
        max_frame_size: int = 16_777_216,
        ssl: bool = False,
        ssl_cafile: str = "",
        ssl_certfile: str = "",
        ssl_keyfile: str = "",
        ssl_insecure: bool = False,
        server_hostname: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.max_frame_size = max_frame_size
        self._ssl = ssl
        self._ssl_cafile = ssl_cafile
        self._ssl_certfile = ssl_certfile
        self._ssl_keyfile = ssl_keyfile
        self._ssl_insecure = ssl_insecure
        self._server_hostname = server_hostname
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._rid = 0
        self._pending: dict[int, asyncio.Future[tuple[dict[str, Any], bytes]]] = {}
        self.deliveries: asyncio.Queue[tuple[dict[str, Any], bytes]] = asyncio.Queue()
        self.pubsub: asyncio.Queue[tuple[dict[str, Any], bytes]] = asyncio.Queue()
        self._reader_task: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def connected(self) -> bool:
        return self._writer is not None and not self._closed

    async def connect(self) -> None:
        ssl_ctx = client_ssl_context(
            enabled=self._ssl,
            cafile=self._ssl_cafile,
            certfile=self._ssl_certfile,
            keyfile=self._ssl_keyfile,
            insecure=self._ssl_insecure,
        )
        kwargs: dict[str, Any] = {}
        if ssl_ctx is not None:
            kwargs["ssl"] = ssl_ctx
            kwargs["server_hostname"] = self._server_hostname or self.host
        self._reader, self._writer = await asyncio.open_connection(
            self.host, self.port, **kwargs
        )
        self._closed = False
        self._reader_task = asyncio.create_task(self._read_loop())

    async def close(self) -> None:
        self._closed = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()

    async def request(
        self,
        cmd: str,
        extra: dict[str, Any] | None = None,
        payload: bytes = b"",
        timeout: float = 30.0,
    ) -> tuple[dict[str, Any], bytes]:
        if self._writer is None:
            raise BrokerError("INTERNAL_ERROR", "not connected")
        self._rid += 1
        rid = self._rid
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[tuple[dict[str, Any], bytes]] = loop.create_future()
        self._pending[rid] = fut
        self._writer.write(command_frame(rid, cmd, extra, payload))
        await self._writer.drain()
        try:
            meta, body = await asyncio.wait_for(fut, timeout=timeout)
        except TimeoutError as exc:
            self._pending.pop(rid, None)
            raise BrokerError("INTERNAL_ERROR", f"timeout waiting for {cmd}") from exc
        if not meta.get("ok", False):
            raise BrokerError(str(meta.get("code") or "INTERNAL_ERROR"), str(meta.get("message") or ""))
        return meta, body

    async def _read_loop(self) -> None:
        assert self._reader is not None
        reader = FrameReader(self.max_frame_size)
        try:
            while not self._closed:
                data = await self._reader.read(65536)
                if not data:
                    break
                try:
                    frames = reader.feed(data)
                except ProtocolError:
                    break
                for frame in frames:
                    if frame.frame_type == FRAME_HEARTBEAT:
                        if self._writer is not None:
                            self._writer.write(heartbeat_frame(frame.request_id))
                        continue
                    meta = parse_metadata(frame)
                    if frame.frame_type == FRAME_RESPONSE:
                        fut = self._pending.pop(frame.request_id, None)
                        if fut is not None and not fut.done():
                            fut.set_result((meta, frame.payload))
                    elif frame.frame_type == FRAME_EVENT:
                        event = meta.get("event")
                        if event == "DELIVER":
                            await self.deliveries.put((meta, frame.payload))
                        elif event == "PUBSUB_MESSAGE":
                            await self.pubsub.put((meta, frame.payload))
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        finally:
            for fut in list(self._pending.values()):
                if not fut.done():
                    fut.set_exception(BrokerError("INTERNAL_ERROR", "connection closed"))
            self._pending.clear()
