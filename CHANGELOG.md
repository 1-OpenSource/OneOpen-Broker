# Changelog

## 0.1.0

Initial V1 release of OneOpen Broker:

- Single-node asyncio message broker
- Durable task queues with ACK/NACK, retries, visibility timeouts, and DLQ
- Direct and fanout routing
- Ephemeral Pub/Sub
- Append-only persistence with snapshots and crash recovery
- Native async/sync Python clients
- Monitoring CLI including live `top`
- Monitoring CLI including live `top`
- Custom Kombu transport for Celery (`oneopen://`)
- Optional TLS 1.2+ and token authentication with role ACLs
