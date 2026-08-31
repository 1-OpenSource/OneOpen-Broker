# OneOpen Broker wire protocol

Version **1**. Length-prefixed binary frames. Application payloads are opaque bytes. Metadata is UTF-8 JSON.

All multi-byte integers are **big-endian**.

## Frame layout

```
Offset  Size  Field
0       4     magic = b"OOB1"
4       1     version = 0x01
5       1     frame_type
6       2     flags (reserved, must be 0 in v1)
8       8     request_id (uint64)
16      4     metadata_len (uint32)
20      4     payload_len (uint32)
24      4     crc32 of (header[0:24] + metadata + payload)
28      N     metadata (UTF-8 JSON object)
28+N    M     payload (raw bytes)
```

Header size is 28 bytes. Default maximum frame size is 16 MiB (`network.max_frame_size`). Oversized or malformed frames close the connection.

CRC32 is IEEE CRC-32 (`zlib.crc32`) of the first 24 header bytes plus metadata plus payload, stored as unsigned 32-bit.

## Frame types

| Value | Name | Direction |
| --- | --- | --- |
| `0x01` | COMMAND | client → server |
| `0x02` | RESPONSE | server → client |
| `0x03` | EVENT | server → client (unsolicited) |
| `0x04` | HEARTBEAT | either; PING/PONG may also use COMMAND/RESPONSE |

## Command metadata

```json
{"cmd": "PUBLISH", "queue": "gpu", "...": "..."}
```

The optional frame payload is the message body.

## Response metadata

Success:

```json
{"ok": true, "...": "..."}
```

Error:

```json
{"ok": false, "code": "NOT_FOUND", "message": "queue does not exist"}
```

The `request_id` of a response matches the command. Events use `request_id = 0` unless they complete a consume reservation tied to a request.

## Events

```json
{"event": "DELIVER", "message_id": "...", "queue": "...", "attempts": 1}
```

```json
{"event": "PUBSUB_MESSAGE", "channel": "events"}
```

Payload is the message body.

## Error codes

`OK`, `INVALID_COMMAND`, `INVALID_FRAME`, `NOT_FOUND`, `QUEUE_FULL`, `ALREADY_EXISTS`, `NOT_BOUND`, `NOT_CONSUMER`, `UNKNOWN_DELIVERY`, `PREFETCH_LIMIT`, `ALREADY_ACKED`, `SLOW_CONSUMER`, `FRAME_TOO_LARGE`, `UNAUTHORIZED`, `FORBIDDEN`, `INTERNAL_ERROR`.

Python stack traces are never sent to clients.

## Commands

Broker: `PING`, `AUTH`, `INFO`, `STATS`

Exchanges: `DECLARE_EXCHANGE`, `DELETE_EXCHANGE`

Queues: `DECLARE_QUEUE`, `DELETE_QUEUE`, `QUEUE_INFO`, `LIST_QUEUES`

Routing: `BIND`, `UNBIND`, `PUBLISH`

Consumers: `CONSUME`, `CANCEL`, `ACK`, `NACK`, `QOS`, `LIST_CONSUMERS`

Pub/Sub: `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH_CHANNEL`, `LIST_CHANNELS`

DLQ: `DLQ_INFO`, `DLQ_REQUEUE`

## Heartbeats

A `PING` command is answered with `{"ok": true, "pong": true}`. Idle connections may also exchange `HEARTBEAT` frames with empty metadata `{}`. Heartbeats and `PING` do not require authentication.

## Authentication

When `security.auth.enabled` is true, clients must send `AUTH` before any command other than `PING` / `AUTH`.

```json
{"cmd": "AUTH", "token": "..."}
```

or

```json
{"cmd": "AUTH", "username": "app", "password": "..."}
```

Success: `{"ok": true, "user": "app", "role": "producer"}`.

Failure: `UNAUTHORIZED`. Five consecutive failures close the connection.

Roles: `admin`, `producer`, `consumer`, `subscriber`, `monitor`. Unauthorized commands return `FORBIDDEN`.

## TLS

The TCP listener may wrap connections in TLS 1.2+ (`security.tls`). The protocol itself is unchanged; TLS is the transport.

## AOF records

Persistence uses a parallel layout with magic `OAOF`. A trailing partial record after a crash is truncated. See `oneopen_broker.persistence.records`.
