"""Command dispatch against BrokerRuntime."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from oneopen_broker.broker.runtime import BrokerRuntime
from oneopen_broker.core.results import AckResult, EnqueueResult, NackResult
from oneopen_broker.monitoring import introspection
from oneopen_broker.protocol.codec import event_frame, response_frame
from oneopen_broker.protocol.errors import (
    ALREADY_ACKED,
    FORBIDDEN,
    INTERNAL_ERROR,
    INVALID_COMMAND,
    NOT_CONSUMER,
    NOT_FOUND,
    QUEUE_FULL,
    UNAUTHORIZED,
    UNKNOWN_DELIVERY,
)
from oneopen_broker.security.acl import authorize
from oneopen_broker.security.auth import Principal, authenticate

log = logging.getLogger("oneopen_broker.broker.handler")


class ConnectionContext:
    def __init__(self, connection_id: str, send: Any, try_send: Any) -> None:
        self.connection_id = connection_id
        self.send = send
        self.try_send = try_send
        self.prefetch = 0
        self.principal: Principal | None = None
        self.auth_failures = 0
        self.must_close = False


def _ok(request_id: int, extra: dict[str, Any] | None = None, payload: bytes = b"") -> bytes:
    return response_frame(request_id, True, extra, payload)


def _err(request_id: int, code: str, message: str = "") -> bytes:
    return response_frame(request_id, False, code=code, message=message or code)


class Handler:
    def __init__(self, runtime: BrokerRuntime) -> None:
        self.runtime = runtime

    def handle(
        self,
        ctx: ConnectionContext,
        request_id: int,
        meta: dict[str, Any],
        payload: bytes,
    ) -> bytes | None:
        cmd = meta.get("cmd")
        if not cmd:
            return _err(request_id, INVALID_COMMAND, "missing cmd")
        auth_required = self.runtime.config.security.auth.enabled
        allowed, code, message = authorize(
            ctx.principal, str(cmd), auth_required=auth_required
        )
        if not allowed:
            if code == UNAUTHORIZED:
                ctx.auth_failures += 1
                if ctx.auth_failures >= 5:
                    ctx.must_close = True
            return _err(request_id, code or FORBIDDEN, message)
        try:
            method = getattr(self, f"cmd_{cmd.lower()}", None)
            if method is None:
                return _err(request_id, INVALID_COMMAND, f"unknown command {cmd}")
            return method(ctx, request_id, meta, payload)
        except Exception:
            log.exception("command %s failed", cmd)
            return _err(request_id, INTERNAL_ERROR, "internal error")

    def cmd_ping(self, ctx, request_id, meta, payload) -> bytes:
        return _ok(request_id, {"pong": True})

    def cmd_auth(self, ctx, request_id, meta, payload) -> bytes:
        if not self.runtime.config.security.auth.enabled:
            ctx.principal = Principal(name="anonymous", role="admin")
            return _ok(request_id, {"user": ctx.principal.name, "role": ctx.principal.role})
        principal = authenticate(
            token=meta.get("token"),
            username=meta.get("username") or meta.get("user"),
            password=meta.get("password"),
            users=self.runtime.config.security.auth.users,
        )
        if principal is None:
            ctx.auth_failures += 1
            if ctx.auth_failures >= 5:
                ctx.must_close = True
            return _err(request_id, UNAUTHORIZED, "invalid credentials")
        ctx.principal = principal
        ctx.auth_failures = 0
        log.info("authenticated user=%s role=%s", principal.name, principal.role)
        return _ok(request_id, {"user": principal.name, "role": principal.role})

    def cmd_info(self, ctx, request_id, meta, payload) -> bytes:
        from oneopen_broker import __version__

        return _ok(
            request_id,
            {
                "version": __version__,
                "host": self.runtime.config.server.host,
                "port": self.runtime.config.server.port,
            },
        )

    def cmd_stats(self, ctx, request_id, meta, payload) -> bytes:
        return _ok(request_id, self.runtime.metrics.broker_stats(self.runtime))

    def cmd_declare_exchange(self, ctx, request_id, meta, payload) -> bytes:
        name = meta.get("name") or ""
        type_ = meta.get("type") or "direct"
        try:
            self.runtime.routing.declare_exchange(name, type_)
        except ValueError as exc:
            return _err(request_id, INVALID_COMMAND, str(exc))
        return _ok(request_id, {"name": name, "type": type_})

    def cmd_delete_exchange(self, ctx, request_id, meta, payload) -> bytes:
        name = meta.get("name") or ""
        if not self.runtime.routing.delete_exchange(name):
            return _err(request_id, NOT_FOUND, "exchange not found")
        return _ok(request_id)

    def cmd_declare_queue(self, ctx, request_id, meta, payload) -> bytes:
        name = meta.get("name")
        if not name:
            return _err(request_id, INVALID_COMMAND, "name required")
        self.runtime.queues.declare_queue(
            name,
            max_length=meta.get("max_length"),
            visibility_timeout=meta.get("visibility_timeout"),
            max_attempts=meta.get("max_attempts"),
        )
        self.runtime.routing.on_queue_declared(name)
        return _ok(request_id, {"name": name})

    def cmd_delete_queue(self, ctx, request_id, meta, payload) -> bytes:
        name = meta.get("name") or ""
        if not self.runtime.queues.delete_queue(name):
            return _err(request_id, NOT_FOUND, "queue not found")
        return _ok(request_id)

    def cmd_queue_info(self, ctx, request_id, meta, payload) -> bytes:
        name = meta.get("name") or ""
        info = self.runtime.queues.queue_info(name)
        if info is None:
            return _err(request_id, NOT_FOUND, "queue not found")
        return _ok(request_id, info)

    def cmd_list_queues(self, ctx, request_id, meta, payload) -> bytes:
        return _ok(request_id, {"queues": introspection.list_queues(self.runtime)})

    def cmd_bind(self, ctx, request_id, meta, payload) -> bytes:
        ok = self.runtime.routing.bind(
            meta.get("exchange") or "",
            meta.get("queue") or "",
            meta.get("routing_key") or "",
        )
        if not ok:
            return _err(request_id, NOT_FOUND, "exchange or queue not found")
        return _ok(request_id)

    def cmd_unbind(self, ctx, request_id, meta, payload) -> bytes:
        ok = self.runtime.routing.unbind(
            meta.get("exchange") or "",
            meta.get("queue") or "",
            meta.get("routing_key") or "",
        )
        if not ok:
            return _err(request_id, NOT_FOUND, "binding not found")
        return _ok(request_id)

    def cmd_publish(self, ctx, request_id, meta, payload) -> bytes:
        message_id, routed, result = self.runtime.routing.publish(
            payload,
            exchange=meta.get("exchange"),
            routing_key=meta.get("routing_key") or "",
            queue=meta.get("queue"),
            max_attempts=meta.get("max_attempts"),
            priority=int(meta.get("priority") or 0),
            headers=meta.get("headers"),
            message_id=meta.get("message_id"),
        )
        if result is EnqueueResult.QUEUE_FULL:
            return _err(request_id, QUEUE_FULL, "queue is full")
        if result is EnqueueResult.NOT_FOUND or not routed:
            return _err(request_id, NOT_FOUND, "not bound")
        for queue_name in routed:
            self.pump_queue(queue_name)
        return _ok(request_id, {"id": message_id, "routed": routed})

    def cmd_get(self, ctx, request_id, meta, payload) -> bytes:
        queue = meta.get("queue")
        if not queue:
            return _err(request_id, INVALID_COMMAND, "queue required")
        consumer_id = meta.get("consumer_id") or f"get-{ctx.connection_id}-{queue}"
        prefetch = (
            int(meta["prefetch"])
            if meta.get("prefetch") is not None
            else int(ctx.prefetch or 0)
        )
        existing = self.runtime.consumers.get(consumer_id)
        if existing is None:
            self.runtime.consumers.register(
                consumer_id,
                ctx.connection_id,
                [queue],
                prefetch_count=prefetch,
            )
        elif prefetch:
            existing.qos.set_prefetch(prefetch)
        item = self.runtime.consumers.try_reserve(queue)
        if item is None:
            return _ok(request_id, {"empty": True})
        consumer, message = item
        return _ok(
            request_id,
            {
                "empty": False,
                "message_id": message.id,
                "queue": message.queue,
                "attempts": message.attempts,
                "headers": message.headers,
                "consumer_id": consumer.consumer_id,
            },
            payload=message.payload,
        )

    def cmd_consume(self, ctx, request_id, meta, payload) -> bytes:
        queues = meta.get("queues") or ([meta["queue"]] if meta.get("queue") else [])
        if not queues:
            return _err(request_id, INVALID_COMMAND, "queue required")
        for name in queues:
            if self.runtime.queues.get_queue(name) is None:
                self.runtime.queues.declare_queue(name)
                self.runtime.routing.on_queue_declared(name)
        consumer_id = meta.get("consumer_id") or str(uuid.uuid4())
        prefetch_raw = meta.get("prefetch")
        if prefetch_raw is None:
            prefetch = int(ctx.prefetch or 1)
        else:
            prefetch = int(prefetch_raw)
        self.runtime.consumers.register(
            consumer_id,
            ctx.connection_id,
            queues,
            prefetch_count=prefetch,
            tag=meta.get("tag") or "",
        )
        for name in queues:
            self.pump_queue(name)
        return _ok(request_id, {"consumer_id": consumer_id, "prefetch": prefetch})

    def cmd_cancel(self, ctx, request_id, meta, payload) -> bytes:
        consumer_id = meta.get("consumer_id")
        if not consumer_id:
            return _err(request_id, INVALID_COMMAND, "consumer_id required")
        if not self.runtime.consumers.cancel(consumer_id):
            return _err(request_id, NOT_CONSUMER, "unknown consumer")
        return _ok(request_id)

    def cmd_ack(self, ctx, request_id, meta, payload) -> bytes:
        message_id = meta.get("message_id") or meta.get("id")
        if not message_id:
            return _err(request_id, INVALID_COMMAND, "message_id required")
        consumer_id = self.runtime.queues.inflight_consumer(message_id)
        result = self.runtime.queues.ack(message_id)
        if result is AckResult.ALREADY_ACKED:
            return _err(request_id, ALREADY_ACKED, "already acked")
        if result is AckResult.NOT_FOUND:
            return _err(request_id, NOT_FOUND, "unknown delivery")
        if consumer_id:
            self.runtime.consumers.on_ack(consumer_id)
            consumer = self.runtime.consumers.get(consumer_id)
            if consumer:
                for name in consumer.queues:
                    self.pump_queue(name)
        return _ok(request_id)

    def cmd_nack(self, ctx, request_id, meta, payload) -> bytes:
        message_id = meta.get("message_id") or meta.get("id")
        if not message_id:
            return _err(request_id, INVALID_COMMAND, "message_id required")
        requeue = bool(meta.get("requeue", True))
        consumer_id = self.runtime.queues.inflight_consumer(message_id)
        result = self.runtime.queues.nack(message_id, requeue=requeue)
        if result is not NackResult.OK:
            return _err(request_id, UNKNOWN_DELIVERY, "unknown delivery")
        if consumer_id:
            self.runtime.consumers.on_ack(consumer_id)
            consumer = self.runtime.consumers.get(consumer_id)
            if consumer:
                for name in consumer.queues:
                    self.pump_queue(name)
        return _ok(request_id)

    def cmd_qos(self, ctx, request_id, meta, payload) -> bytes:
        prefetch = int(meta.get("prefetch") or 0)
        ctx.prefetch = prefetch
        consumer_id = meta.get("consumer_id")
        if consumer_id:
            if not self.runtime.consumers.qos(consumer_id, prefetch):
                return _err(request_id, NOT_CONSUMER, "unknown consumer")
        else:
            for consumer in self.runtime.consumers.list_consumers():
                if consumer.connection_id == ctx.connection_id:
                    consumer.qos.set_prefetch(prefetch)
        return _ok(request_id, {"prefetch": prefetch})

    def cmd_list_consumers(self, ctx, request_id, meta, payload) -> bytes:
        return _ok(request_id, {"consumers": introspection.list_consumers(self.runtime)})

    def cmd_subscribe(self, ctx, request_id, meta, payload) -> bytes:
        channel = meta.get("channel")
        if not channel:
            return _err(request_id, INVALID_COMMAND, "channel required")

        def send(_data: bytes) -> bool:
            return True

        self.runtime.pubsub.subscribe(ctx.connection_id, channel, send)
        return _ok(request_id, {"channel": channel})

    def cmd_unsubscribe(self, ctx, request_id, meta, payload) -> bytes:
        channel = meta.get("channel") or ""
        self.runtime.pubsub.unsubscribe(ctx.connection_id, channel)
        return _ok(request_id)

    def cmd_publish_channel(self, ctx, request_id, meta, payload) -> bytes:
        channel = meta.get("channel")
        if not channel:
            return _err(request_id, INVALID_COMMAND, "channel required")
        result = self.runtime.pubsub.publish(
            channel,
            payload,
            lambda conn_id, body: self._fanout_pubsub(conn_id, channel, body),
        )
        self._last_slow = list(result["slow"])
        return _ok(request_id, {"delivered": result["delivered"], "slow": result["slow"]})

    def cmd_list_channels(self, ctx, request_id, meta, payload) -> bytes:
        return _ok(request_id, {"channels": self.runtime.pubsub.list_channels()})

    def cmd_dlq_info(self, ctx, request_id, meta, payload) -> bytes:
        name = meta.get("queue") or meta.get("name") or ""
        info = self.runtime.queues.dlq_info(name)
        if info is None:
            return _err(request_id, NOT_FOUND, "queue not found")
        return _ok(request_id, info)

    def cmd_dlq_requeue(self, ctx, request_id, meta, payload) -> bytes:
        name = meta.get("queue") or meta.get("name") or ""
        message_id = meta.get("message_id") or meta.get("id")
        if not message_id:
            return _err(request_id, INVALID_COMMAND, "message_id required")
        if not self.runtime.queues.dlq_requeue(name, message_id):
            return _err(request_id, NOT_FOUND, "message not in dlq")
        self.pump_queue(name)
        return _ok(request_id)

    def _fanout_pubsub(self, connection_id: str, channel: str, body: bytes) -> bool:
        frame = event_frame("PUBSUB_MESSAGE", {"channel": channel}, payload=body)
        sender = self._senders.get(connection_id)
        if sender is None:
            return False
        return sender(frame)

    def set_senders(self, senders: dict[str, Any]) -> None:
        self._senders = senders

    _senders: dict[str, Any] = {}

    def pump_queue(self, queue_name: str) -> None:
        while True:
            item = self.runtime.consumers.try_reserve(queue_name)
            if item is None:
                return
            consumer, message = item
            frame = event_frame(
                "DELIVER",
                {
                    "message_id": message.id,
                    "queue": message.queue,
                    "attempts": message.attempts,
                    "headers": message.headers,
                    "consumer_id": consumer.consumer_id,
                    "priority": message.priority,
                },
                payload=message.payload,
            )
            sender = self._senders.get(consumer.connection_id)
            if sender is None or not sender(frame):
                self.runtime.queues.nack(message.id, requeue=True)
                self.runtime.consumers.on_ack(consumer.consumer_id)
                return
