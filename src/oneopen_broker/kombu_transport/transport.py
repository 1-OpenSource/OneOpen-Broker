"""Kombu virtual transport backed by OneOpen Broker."""

from __future__ import annotations

from queue import Empty
from typing import Any

from kombu.transport import virtual
from kombu.utils.json import dumps, loads

from oneopen_broker.client.sync_client import Broker
from oneopen_broker.kombu_transport import register_transport

register_transport()


class Channel(virtual.Channel):
    def __init__(self, connection, **kwargs):
        self._client: Broker | None = None
        super().__init__(connection, **kwargs)

    def _open(self) -> Broker:
        info = self.connection.client
        host = info.hostname or "127.0.0.1"
        port = int(info.port or 6380)
        token = info.password or None
        username = info.userid or None
        if username == "":
            username = None
        if username and not info.password:
            token = username
            username = None
        ssl_opt = getattr(info, "ssl", None)
        ssl_enabled = bool(ssl_opt)
        ssl_cafile = ""
        ssl_certfile = ""
        ssl_keyfile = ""
        ssl_insecure = False
        if isinstance(ssl_opt, dict):
            ssl_enabled = True
            ssl_cafile = ssl_opt.get("ca_certs") or ssl_opt.get("cafile") or ""
            ssl_certfile = ssl_opt.get("certfile") or ""
            ssl_keyfile = ssl_opt.get("keyfile") or ""
            ssl_insecure = bool(ssl_opt.get("cert_reqs") == "CERT_NONE")
        return Broker(
            host,
            port,
            token=token if not username else None,
            username=username,
            password=info.password if username else None,
            ssl=ssl_enabled,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_insecure=ssl_insecure,
        )

    @property
    def client(self) -> Broker:
        if self._client is None:
            self._client = self._open()
        return self._client

    def _put(self, queue: str, message: dict[str, Any], **kwargs: Any) -> None:
        self.client.publish(dumps(message).encode("utf-8"), queue=queue)

    def _get(self, queue: str, timeout: float | None = None) -> dict[str, Any]:
        got = self.client.get(queue)
        if got is None:
            raise Empty()
        decoded = loads(got.payload.decode("utf-8"))
        headers = decoded.setdefault("headers", {})
        headers["x-oneopen-id"] = got.id
        return decoded

    def _size(self, queue: str) -> int:
        try:
            info = self.client.queue_info(queue)
        except Exception:
            return 0
        return int(info.get("ready") or 0)

    def _purge(self, queue: str) -> int:
        n = 0
        while True:
            got = self.client.get(queue)
            if got is None:
                break
            self.client.ack(got.id)
            n += 1
        return n

    def queue_declare(self, queue, *args, **kwargs):
        name = getattr(queue, "name", queue)
        self.client.declare_queue(str(name))
        try:
            return super().queue_declare(queue, *args, **kwargs)
        except Exception:
            return str(name), self._size(str(name)), 0

    def exchange_declare(self, exchange, type="direct", durable=False, **kwargs):
        name = getattr(exchange, "name", exchange)
        kind = type if type in {"direct", "fanout"} else "direct"
        try:
            self.client.declare_exchange(str(name), kind)
        except Exception:
            pass
        return super().exchange_declare(exchange, type=type, durable=durable, **kwargs)

    def queue_bind(self, queue, exchange, routing_key="", **kwargs):
        qname = getattr(queue, "name", queue)
        ename = getattr(exchange, "name", exchange)
        try:
            self.client.bind(str(ename), str(qname), routing_key or "")
        except Exception:
            pass
        return super().queue_bind(queue, exchange, routing_key, **kwargs)

    def _oneopen_id(self, delivery_tag: int) -> str | None:
        delivered = getattr(self.qos, "_delivered", None) or {}
        item = delivered.get(delivery_tag)
        if item is None:
            return None
        message = item[0] if isinstance(item, tuple) else item
        headers = getattr(message, "headers", None)
        if headers is None and isinstance(message, dict):
            headers = message.get("headers")
        if not headers:
            return None
        return headers.get("x-oneopen-id")

    def basic_ack(self, delivery_tag, multiple=False):
        oid = self._oneopen_id(delivery_tag)
        if oid:
            try:
                self.client.ack(oid)
            except Exception:
                pass
        return super().basic_ack(delivery_tag, multiple)

    def basic_reject(self, delivery_tag, requeue=True):
        oid = self._oneopen_id(delivery_tag)
        if oid:
            try:
                self.client.nack(oid, requeue=bool(requeue))
            except Exception:
                pass
        return super().basic_reject(delivery_tag, requeue=requeue)

    def basic_qos(self, prefetch_size, prefetch_count, apply_global=False):
        result = super().basic_qos(prefetch_size, prefetch_count, apply_global)
        try:
            self.client.qos(int(prefetch_count or 0))
        except Exception:
            pass
        return result

    def close(self):
        client = self._client
        self._client = None
        try:
            super().close()
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass


class Transport(virtual.Transport):
    Channel = Channel
    driver_type = "oneopen"
    driver_name = "oneopen"
    polling_interval = 0.05
    implements = virtual.Transport.implements.extend(
        asynchronous=False,
        exchange_type=frozenset(["direct", "fanout", "topic"]),
    )


# Kombu entry-point target
channel = Channel
