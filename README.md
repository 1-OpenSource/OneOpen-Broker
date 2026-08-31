# OneOpen Broker

A lightweight, Python-native, single-node message broker with first-class Celery support, durable task queues, Pub/Sub, and built-in monitoring — without Redis or RabbitMQ.

OneOpen Broker is **not** a Redis, RabbitMQ, or Kafka replacement. Those products solve broader problems. V1 is a focused broker for single-server Python applications talking to multiple Celery workers.

## Install

```bash
pip install oneopen-broker
```

Celery support:

```bash
pip install "oneopen-broker[celery]"
```

## Start the broker

```bash
oneopen-broker start
```

Default listen address: `127.0.0.1:6380`.

```bash
oneopen-broker start --host 127.0.0.1 --port 6380 --data-dir ./data
```

## Celery

```python
import oneopen_broker  # registers the oneopen:// Kombu transport
from celery import Celery

app = Celery("myapp", broker="oneopen://127.0.0.1:6380")
```

```bash
celery -A myapp worker --concurrency=4
```

## Native client

```python
from oneopen_broker import AsyncBroker

broker = AsyncBroker("127.0.0.1", 6380)
await broker.connect()
await broker.publish(queue="gpu", payload=b"...")

message = await broker.consume("gpu")
try:
    await process(message.payload)
    await message.ack()
except Exception:
    await message.nack(requeue=True)
```

Pub/Sub is ephemeral: disconnected subscribers miss messages published while they are away.

```python
async for event in broker.subscribe("events"):
    print(event.payload)
```

## Delivery semantics

V1 provides **at-least-once** delivery. Consumers must tolerate duplicates after crashes or ambiguous acknowledgements. Exactly-once delivery is not claimed.

## Durability

Persistence is an append-only file plus periodic snapshots. No external database.

| `persistence.fsync` | Guarantee |
| --- | --- |
| `always` | Publish/ACK responses wait until the record is fsync'd |
| `everysec` (default) | Responses return after the in-memory commit; disk is fsync'd at most once per second. Up to about one second of acknowledged work may be lost on a crash |
| `none` | OS page cache only; fastest, weakest |

Inflight messages at broker crash are recovered as deliverable (ACK was not proven).

## Configuration

YAML file, environment variables (`ONEOPEN_*`), and CLI flags. See `oneopen-broker start --help`.

```yaml
server:
  host: 127.0.0.1
  port: 6380
persistence:
  directory: ./data
  fsync: everysec
  snapshot_interval: 300
queues:
  default_visibility_timeout: 300
  default_max_attempts: 3
network:
  max_connections: 10000
  max_frame_size: 16777216
pubsub:
  subscriber_buffer: 1000
security:
  tls:
    enabled: false
  auth:
    enabled: false
```

## Monitoring

```bash
oneopen-broker stats
oneopen-broker queues
oneopen-broker queue gpu_tasks
oneopen-broker consumers
oneopen-broker channels
oneopen-broker dlq gpu_tasks
oneopen-broker top
```

The CLI talks to the broker over the same protocol as clients. It does not read memory or persistence files.

## Security

V1 is intended for private networks. Bind to localhost by default. Optionally require a token and TLS:

```yaml
security:
  tls:
    enabled: true
    certfile: /etc/oneopen/server.crt
    keyfile: /etc/oneopen/server.key
    cafile: /etc/oneopen/ca.crt
    require_client_cert: false
  auth:
    enabled: true
    users:
      - name: app
        token: change-me
        role: producer
      - name: worker
        token: change-me-too
        role: consumer
      - name: ops
        token: change-me-admin
        role: admin
```

Roles: `admin`, `producer`, `consumer`, `subscriber`, `monitor`.

```bash
oneopen-broker start --auth-token "$ONEOPEN_AUTH_TOKEN" --tls-cert server.crt --tls-key server.key
oneopen-broker stats --token "$ONEOPEN_AUTH_TOKEN"
```

```python
broker = AsyncBroker("127.0.0.1", 6380, token="change-me", ssl=True, ssl_cafile="ca.crt")
```

Celery (token in the URL password; empty user):

```python
app = Celery("myapp", broker="oneopen://:change-me@127.0.0.1:6380")
```

Credentials are compared with a timing-safe digest and are never logged. Auth is off unless you enable it, so existing local setups keep working.

## Requirements

- Python 3.11+
- One process, one asyncio event loop, one server

V1 does not cluster, replicate, or speak the Redis protocol.
