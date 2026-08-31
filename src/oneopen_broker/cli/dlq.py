from __future__ import annotations

from oneopen_broker.client.sync_client import Broker


def print_dlq(client: Broker, queue: str, requeue_id: str | None) -> None:
    if requeue_id:
        client.dlq_requeue(queue, requeue_id)
        print(f"requeued {requeue_id} onto {queue}")
        return
    info = client.dlq_info(queue)
    print(f"DLQ {info.get('dlq')} count={info.get('count')}")
    for msg in info.get("messages") or []:
        print(f"  {msg['id']} attempts={msg['attempts']}")
