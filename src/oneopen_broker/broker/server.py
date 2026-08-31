"""Asyncio TCP broker server."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

from oneopen_broker.broker.config import BrokerConfig
from oneopen_broker.broker.handler import ConnectionContext, Handler
from oneopen_broker.broker.runtime import BrokerRuntime
from oneopen_broker.persistence.aof import AOFWriter
from oneopen_broker.protocol.codec import event_frame, heartbeat_frame, parse_metadata, response_frame
from oneopen_broker.protocol.errors import SLOW_CONSUMER, ProtocolError
from oneopen_broker.protocol.frames import FRAME_COMMAND, FRAME_HEARTBEAT, FrameReader
from oneopen_broker.security.tls import server_ssl_context

log = logging.getLogger("oneopen_broker.broker.server")


class ClientConnection:
    def __init__(
        self,
        connection_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        max_frame_size: int,
        outbound_limit: int,
        metrics: Any,
    ) -> None:
        self.connection_id = connection_id
        self.reader = reader
        self.writer = writer
        self.frame_reader = FrameReader(max_frame_size)
        self.outbound: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=outbound_limit)
        self.connected_at = time.time()
        self.last_activity = self.connected_at
        peer = writer.get_extra_info("peername")
        self.remote = f"{peer[0]}:{peer[1]}" if peer else ""
        self.metrics = metrics
        self.closed = False
        self._writer_task: asyncio.Task[None] | None = None

    def try_send(self, data: bytes) -> bool:
        if self.closed:
            return False
        try:
            self.outbound.put_nowait(data)
            return True
        except asyncio.QueueFull:
            return False

    async def send(self, data: bytes) -> None:
        if self.closed:
            return
        await self.outbound.put(data)

    def start_writer(self) -> None:
        self._writer_task = asyncio.create_task(self._write_loop(), name=f"write-{self.connection_id}")

    async def _write_loop(self) -> None:
        try:
            while True:
                data = await self.outbound.get()
                if data is None:
                    break
                self.writer.write(data)
                await self.writer.drain()
                self.metrics.add_sent(len(data))
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.outbound.put_nowait(None)
        except asyncio.QueueFull:
            # drop one to send sentinel
            try:
                _ = self.outbound.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self.outbound.put_nowait(None)
            except asyncio.QueueFull:
                pass
        if self._writer_task is not None:
            try:
                await asyncio.wait_for(self._writer_task, timeout=2)
            except (TimeoutError, asyncio.CancelledError):
                self._writer_task.cancel()


class BrokerServer:
    def __init__(self, config: BrokerConfig, runtime: BrokerRuntime) -> None:
        self.config = config
        self.runtime = runtime
        self.handler = Handler(runtime)
        self.connections: dict[str, ClientConnection] = {}
        self._senders: dict[str, Any] = {}
        self.handler.set_senders(self._senders)
        self.handler.disconnect_fn = self._disconnect_slow
        self._server: asyncio.AbstractServer | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._accepting = False
        self._delivering = True
        self.bound_host = config.server.host
        self.bound_port = config.server.port
        self._halt = asyncio.Event()

    def _disconnect_slow(self, connection_id: str) -> None:
        conn = self.connections.get(connection_id)
        if conn is None:
            return
        conn.try_send(
            event_frame("SLOW_CONSUMER", {"code": SLOW_CONSUMER})
        )
        asyncio.create_task(self._drop(conn, "slow consumer"))

    async def start(self) -> None:
        self._halt = asyncio.Event()
        self._accepting = True
        self.runtime.ready = True
        ssl_ctx = server_ssl_context(self.config.security.tls)
        self._server = await asyncio.start_server(
            self._on_connect,
            self.config.server.host,
            self.config.server.port,
            limit=self.config.network.max_frame_size + 4096,
            ssl=ssl_ctx,
        )
        sock = self._server.sockets[0]
        self.bound_host, self.bound_port = sock.getsockname()[:2]
        self._tasks.append(asyncio.create_task(self._ticker(), name="visibility-ticker"))
        if self.config.security.tls.enabled:
            log.info("TLS enabled")
        if self.config.security.auth.enabled:
            log.info("authentication required")
            if not self.config.security.auth.users:
                log.error("authentication is enabled but no users are configured")
        elif self.config.server.host not in {"127.0.0.1", "::1", "localhost"}:
            log.warning(
                "listening on %s without authentication; set security.auth.enabled",
                self.config.server.host,
            )
        log.info(
            "broker listening on %s:%s",
            self.bound_host,
            self.bound_port,
        )

    async def stop(self) -> None:
        self._accepting = False
        self._delivering = False
        self.runtime.ready = False
        self._halt.set()
        for conn in list(self.connections.values()):
            conn.closed = True
            try:
                conn.writer.transport.abort()
            except Exception:
                try:
                    conn.writer.close()
                except Exception:
                    pass
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._server is not None:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=5)
            except TimeoutError:
                log.warning("server wait_closed timed out")
            self._server = None
        for conn in list(self.connections.values()):
            await conn.close()
        self.connections.clear()

    async def _ticker(self) -> None:
        while True:
            await asyncio.sleep(0.05)
            if not self._delivering:
                continue
            try:
                self.runtime.queues.tick()
                for name in self.runtime.queues.list_queues():
                    self.handler.pump_queue(name)
            except Exception:
                log.exception("ticker failed")

    async def _on_connect(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        if not self._accepting or not self.runtime.ready:
            writer.close()
            await writer.wait_closed()
            return
        if self.runtime.metrics.connections >= self.config.network.max_connections:
            writer.close()
            await writer.wait_closed()
            return
        connection_id = str(uuid.uuid4())
        conn = ClientConnection(
            connection_id,
            reader,
            writer,
            self.config.network.max_frame_size,
            self.config.network.outbound_buffer,
            self.runtime.metrics,
        )
        self.connections[connection_id] = conn
        self._senders[connection_id] = conn.try_send
        self.runtime.metrics.on_connect()
        conn.start_writer()
        log.info("connection %s from %s", connection_id, conn.remote)
        try:
            await self._read_loop(conn)
        finally:
            await self._drop(conn, "disconnect")

    async def _drop(self, conn: ClientConnection, reason: str) -> None:
        if conn.connection_id not in self.connections:
            await conn.close()
            return
        log.info("closing %s (%s)", conn.connection_id, reason)
        self.connections.pop(conn.connection_id, None)
        self._senders.pop(conn.connection_id, None)
        self.runtime.pubsub.disconnect(conn.connection_id)
        self.runtime.consumers.disconnect_connection(conn.connection_id)
        self.runtime.metrics.on_disconnect()
        await conn.close()

    async def _read_loop(self, conn: ClientConnection) -> None:
        ctx = ConnectionContext(conn.connection_id, conn.send, conn.try_send)
        while not conn.closed and not self._halt.is_set():
            try:
                read_task = asyncio.create_task(conn.reader.read(65536))
                halt_task = asyncio.create_task(self._halt.wait())
                done, pending = await asyncio.wait(
                    {read_task, halt_task},
                    timeout=self.config.network.idle_timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                if halt_task in done or self._halt.is_set():
                    return
                if not done:
                    if time.time() - conn.last_activity > self.config.network.idle_timeout:
                        return
                    continue
                data = read_task.result() if read_task in done else b""
            except (TimeoutError, ConnectionError, asyncio.CancelledError, Exception):
                return
            if not data:
                return
            self.runtime.metrics.add_received(len(data))
            conn.last_activity = time.time()
            try:
                frames = conn.frame_reader.feed(data)
            except ProtocolError as exc:
                await conn.send(
                    response_frame(0, False, code=exc.code, message=exc.message)
                )
                return
            for frame in frames:
                if frame.frame_type == FRAME_HEARTBEAT:
                    conn.try_send(heartbeat_frame(frame.request_id))
                    continue
                if frame.frame_type != FRAME_COMMAND:
                    continue
                try:
                    meta = parse_metadata(frame)
                except ProtocolError as exc:
                    await conn.send(
                        response_frame(
                            frame.request_id, False, code=exc.code, message=exc.message
                        )
                    )
                    return
                reply = self.handler.handle(ctx, frame.request_id, meta, frame.payload)
                if reply is not None:
                    if not conn.try_send(reply):
                        await conn.send(
                            event_frame("SLOW_CONSUMER", {"code": SLOW_CONSUMER})
                        )
                        return
                if ctx.must_close:
                    return
                journal = self.runtime.journal
                if isinstance(journal, AOFWriter):
                    await journal.wait_durable(journal.seq)
                slow = getattr(self.handler, "_last_slow", None)
                if slow:
                    self.handler._last_slow = []
                    for conn_id in slow:
                        self._disconnect_slow(conn_id)


# end of BrokerServer
