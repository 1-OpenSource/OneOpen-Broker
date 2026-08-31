"""Durable queue engine. All state is owned by the asyncio event loop."""

from __future__ import annotations

import heapq
import logging
import time
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from oneopen_broker.core.delayed import DelayedHeap
from oneopen_broker.core.dlq import dlq_name, is_dlq, origin_queue
from oneopen_broker.core.inflight import InflightRecord
from oneopen_broker.core.message import Message
from oneopen_broker.core.results import AckResult, EnqueueResult, NackResult
from oneopen_broker.core.retry import retry_delay

log = logging.getLogger("oneopen_broker.core.queue")


class Journal(Protocol):
    def append(self, op: str, meta: dict[str, Any], payload: bytes = b"") -> None: ...


class NullJournal:
    def append(self, op: str, meta: dict[str, Any], payload: bytes = b"") -> None:
        return None


@dataclass(slots=True)
class QueueStats:
    ready: int = 0
    inflight: int = 0
    delayed: int = 0
    dlq: int = 0
    consumers: int = 0
    published: int = 0
    delivered: int = 0
    acked: int = 0
    nacked: int = 0
    retried: int = 0
    dead: int = 0


@dataclass(slots=True)
class QueueSpec:
    name: str
    durable: bool = True
    max_length: int | None = None
    visibility_timeout: float = 300.0
    max_attempts: int = 3
    dead_letter: bool = True


@dataclass(slots=True)
class DurableQueue:
    spec: QueueSpec
    ready: list[tuple[int, int, str]] = field(default_factory=list)
    delayed_ids: set[str] = field(default_factory=set)
    inflight: dict[str, InflightRecord] = field(default_factory=dict)
    dlq_ids: deque[str] = field(default_factory=deque)
    messages: dict[str, Message] = field(default_factory=dict)
    stats: QueueStats = field(default_factory=QueueStats)
    consumer_ids: list[str] = field(default_factory=list)
    rr_index: int = 0

    @property
    def name(self) -> str:
        return self.spec.name

    def ready_count(self) -> int:
        return len(self.ready)

    def inflight_count(self) -> int:
        return len(self.inflight)

    def delayed_count(self) -> int:
        return len(self.delayed_ids)

    def dlq_count(self) -> int:
        return len(self.dlq_ids)

    def depth(self) -> int:
        return self.ready_count() + self.inflight_count() + self.delayed_count()


class QueueEngine:
    def __init__(
        self,
        *,
        visibility_timeout: float = 300.0,
        max_attempts: int = 3,
        max_length: int | None = None,
        retry_backoff: str = "exponential",
        retry_base_delay: float = 1.0,
        recent_ack_limit: int = 100_000,
        journal: Journal | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.visibility_timeout = visibility_timeout
        self.max_attempts = max_attempts
        self.max_length = max_length
        self.retry_backoff = retry_backoff
        self.retry_base_delay = retry_base_delay
        self.journal: Journal = journal or NullJournal()
        self._now = clock or time.time
        self._queues: dict[str, DurableQueue] = {}
        self._by_id: dict[str, str] = {}
        self._recent_acks: OrderedDict[str, None] = OrderedDict()
        self._recent_ack_limit = recent_ack_limit
        self._seq = 0
        self._visibility_heap: list[tuple[float, int, str, str]] = []
        self._delayed = DelayedHeap()

    def now(self) -> float:
        return self._now()

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def declare_queue(
        self,
        name: str,
        *,
        durable: bool = True,
        max_length: int | None = None,
        visibility_timeout: float | None = None,
        max_attempts: int | None = None,
        dead_letter: bool = True,
        persist: bool = True,
    ) -> DurableQueue:
        existing = self._queues.get(name)
        if existing is not None:
            return existing
        spec = QueueSpec(
            name=name,
            durable=durable,
            max_length=max_length if max_length is not None else self.max_length,
            visibility_timeout=(
                visibility_timeout
                if visibility_timeout is not None
                else self.visibility_timeout
            ),
            max_attempts=max_attempts if max_attempts is not None else self.max_attempts,
            dead_letter=dead_letter and not is_dlq(name),
        )
        queue = DurableQueue(spec=spec)
        self._queues[name] = queue
        if spec.dead_letter:
            self.declare_queue(
                dlq_name(name),
                durable=True,
                max_length=None,
                visibility_timeout=spec.visibility_timeout,
                max_attempts=1,
                dead_letter=False,
                persist=persist,
            )
        if persist:
            self.journal.append(
                "QUEUE_DECLARE",
                {
                    "name": name,
                    "durable": durable,
                    "max_length": spec.max_length,
                    "visibility_timeout": spec.visibility_timeout,
                    "max_attempts": spec.max_attempts,
                    "dead_letter": spec.dead_letter,
                },
            )
        return queue

    def delete_queue(self, name: str, *, persist: bool = True) -> bool:
        queue = self._queues.pop(name, None)
        if queue is None:
            return False
        for message_id in list(queue.messages):
            self._by_id.pop(message_id, None)
        if persist:
            self.journal.append("QUEUE_DELETE", {"name": name})
        if queue.spec.dead_letter:
            self.delete_queue(dlq_name(name), persist=persist)
        return True

    def get_queue(self, name: str) -> DurableQueue | None:
        return self._queues.get(name)

    def list_queues(self) -> list[str]:
        return sorted(self._queues)

    def enqueue(
        self,
        message: Message,
        *,
        persist: bool = True,
    ) -> EnqueueResult:
        queue = self._queues.get(message.queue)
        if queue is None:
            return EnqueueResult.NOT_FOUND
        limit = queue.spec.max_length
        if limit is not None and queue.depth() >= limit:
            return EnqueueResult.QUEUE_FULL
        available_at = message.available_at
        now = self.now()
        queue.messages[message.id] = message
        self._by_id[message.id] = message.queue
        queue.stats.published += 1
        if available_at is not None and available_at > now:
            queue.delayed_ids.add(message.id)
            self._delayed.push(available_at, self._next_seq(), message.queue, message.id)
        else:
            message.available_at = None
            heapq.heappush(queue.ready, (-message.priority, self._next_seq(), message.id))
        if persist:
            meta = {
                "id": message.id,
                "queue": message.queue,
                "attempts": message.attempts,
                "max_attempts": message.max_attempts,
                "priority": message.priority,
                "created_at": message.created_at,
                "available_at": message.available_at,
                "headers": message.headers,
                "exchange": message.exchange,
                "routing_key": message.routing_key,
            }
            self.journal.append("MESSAGE_PUBLISH", meta, message.payload)
        return EnqueueResult.OK

    def reserve(self, queue_name: str, consumer_id: str) -> Message | None:
        queue = self._queues.get(queue_name)
        if queue is None:
            return None
        while queue.ready:
            _prio, _seq, message_id = heapq.heappop(queue.ready)
            message = queue.messages.get(message_id)
            if message is None or message_id in queue.inflight or message_id in queue.delayed_ids:
                continue
            now = self.now()
            deadline = now + queue.spec.visibility_timeout
            queue.inflight[message_id] = InflightRecord(
                message_id=message_id,
                queue=queue_name,
                consumer_id=consumer_id,
                delivery_time=now,
                visibility_deadline=deadline,
            )
            heapq.heappush(
                self._visibility_heap,
                (deadline, self._next_seq(), queue_name, message_id),
            )
            queue.stats.delivered += 1
            return message
        return None

    def inflight_consumer(self, message_id: str) -> str | None:
        queue_name = self._by_id.get(message_id)
        if queue_name is None:
            return None
        queue = self._queues.get(queue_name)
        if queue is None:
            return None
        record = queue.inflight.get(message_id)
        return record.consumer_id if record else None

    def ack(self, message_id: str, *, persist: bool = True) -> AckResult:
        if message_id in self._recent_acks:
            return AckResult.ALREADY_ACKED
        queue_name = self._by_id.get(message_id)
        if queue_name is None:
            return AckResult.NOT_FOUND
        queue = self._queues.get(queue_name)
        if queue is None:
            return AckResult.NOT_FOUND
        record = queue.inflight.pop(message_id, None)
        if record is None:
            if message_id in queue.messages:
                return AckResult.NOT_FOUND
            return AckResult.NOT_FOUND
        queue.messages.pop(message_id, None)
        self._by_id.pop(message_id, None)
        self._remember_ack(message_id)
        queue.stats.acked += 1
        if persist:
            self.journal.append("MESSAGE_ACK", {"id": message_id, "queue": queue_name})
        return AckResult.OK

    def nack(
        self,
        message_id: str,
        *,
        requeue: bool = True,
        persist: bool = True,
    ) -> NackResult:
        queue_name = self._by_id.get(message_id)
        if queue_name is None:
            if message_id in self._recent_acks:
                return NackResult.NOT_FOUND
            return NackResult.UNKNOWN_DELIVERY
        queue = self._queues.get(queue_name)
        if queue is None:
            return NackResult.NOT_FOUND
        record = queue.inflight.pop(message_id, None)
        if record is None:
            return NackResult.UNKNOWN_DELIVERY
        message = queue.messages.get(message_id)
        if message is None:
            return NackResult.NOT_FOUND
        queue.stats.nacked += 1
        if persist:
            self.journal.append(
                "MESSAGE_NACK",
                {"id": message_id, "queue": queue_name, "requeue": requeue},
            )
        if requeue:
            self._retry_or_dead(queue, message, persist=persist)
        else:
            self._move_to_dlq(queue, message, persist=persist)
        return NackResult.OK

    def recover_consumer(self, consumer_id: str, *, persist: bool = True) -> int:
        recovered = 0
        for queue in self._queues.values():
            timed_out = [
                rec
                for rec in list(queue.inflight.values())
                if rec.consumer_id == consumer_id
            ]
            for rec in timed_out:
                queue.inflight.pop(rec.message_id, None)
                message = queue.messages.get(rec.message_id)
                if message is None:
                    continue
                self._retry_or_dead(queue, message, persist=persist)
                recovered += 1
        return recovered

    def tick(self, now: float | None = None) -> int:
        ts = self.now() if now is None else now
        n = 0
        n += self._release_delayed(ts)
        n += self._timeout_inflight(ts)
        return n

    def dlq_info(self, queue_name: str) -> dict[str, Any] | None:
        target = dlq_name(queue_name) if not is_dlq(queue_name) else queue_name
        queue = self._queues.get(target)
        if queue is None:
            return None
        ids = list(queue.dlq_ids) if queue.dlq_ids else [mid for _, _, mid in queue.ready]
        messages = []
        for message_id in ids[:200]:
            msg = queue.messages.get(message_id)
            if msg is None:
                continue
            messages.append(
                {
                    "id": msg.id,
                    "queue": origin_queue(target),
                    "attempts": msg.attempts,
                    "created_at": msg.created_at,
                    "headers": msg.headers,
                }
            )
        return {
            "queue": origin_queue(target),
            "dlq": target,
            "count": queue.ready_count() + queue.inflight_count() + queue.delayed_count(),
            "messages": messages,
        }

    def dlq_requeue(self, queue_name: str, message_id: str, *, persist: bool = True) -> bool:
        origin = origin_queue(queue_name)
        dlq = self._queues.get(dlq_name(origin))
        dest = self._queues.get(origin)
        if dlq is None or dest is None:
            return False
        message = dlq.messages.pop(message_id, None)
        if message is None:
            return False
        self._by_id.pop(message_id, None)
        self._drop_ready(dlq, message_id)
        if message_id in dlq.dlq_ids:
            dlq.dlq_ids = deque(mid for mid in dlq.dlq_ids if mid != message_id)
        message.queue = origin
        message.attempts = 0
        message.available_at = None
        result = self.enqueue(message, persist=False)
        if result is not EnqueueResult.OK:
            dlq.messages[message_id] = message
            message.queue = dlq.name
            self._by_id[message_id] = dlq.name
            heapq.heappush(dlq.ready, (-message.priority, self._next_seq(), message_id))
            return False
        if persist:
            self.journal.append(
                "MESSAGE_REQUEUE",
                {"id": message_id, "from": dlq.name, "to": origin},
            )
        return True

    def snapshot_state(self) -> dict[str, Any]:
        queues = []
        for queue in self._queues.values():
            messages = []
            for message in queue.messages.values():
                messages.append(
                    {
                        "id": message.id,
                        "queue": message.queue,
                        "payload": message.payload,
                        "created_at": message.created_at,
                        "attempts": message.attempts,
                        "max_attempts": message.max_attempts,
                        "priority": message.priority,
                        "headers": message.headers,
                        "available_at": message.available_at,
                        "exchange": message.exchange,
                        "routing_key": message.routing_key,
                        "state": self._message_state(queue, message.id),
                    }
                )
            queues.append(
                {
                    "spec": {
                        "name": queue.spec.name,
                        "durable": queue.spec.durable,
                        "max_length": queue.spec.max_length,
                        "visibility_timeout": queue.spec.visibility_timeout,
                        "max_attempts": queue.spec.max_attempts,
                        "dead_letter": queue.spec.dead_letter,
                    },
                    "messages": messages,
                    "stats": {
                        "published": queue.stats.published,
                        "delivered": queue.stats.delivered,
                        "acked": queue.stats.acked,
                        "nacked": queue.stats.nacked,
                        "retried": queue.stats.retried,
                        "dead": queue.stats.dead,
                    },
                }
            )
        return {"queues": queues, "seq": self._seq}

    def restore_state(self, state: dict[str, Any]) -> None:
        self._queues.clear()
        self._by_id.clear()
        self._visibility_heap.clear()
        self._delayed = DelayedHeap()
        self._seq = int(state.get("seq", 0))
        for qstate in state.get("queues", []):
            spec_raw = qstate["spec"]
            spec = QueueSpec(**spec_raw)
            queue = DurableQueue(spec=spec)
            self._queues[spec.name] = queue
            stats = qstate.get("stats") or {}
            for key, value in stats.items():
                if hasattr(queue.stats, key):
                    setattr(queue.stats, key, value)
            for raw in qstate.get("messages", []):
                payload = raw["payload"]
                if isinstance(payload, str):
                    payload = payload.encode("utf-8")
                message = Message(
                    id=raw["id"],
                    queue=raw["queue"],
                    payload=payload,
                    created_at=raw["created_at"],
                    attempts=raw.get("attempts", 0),
                    max_attempts=raw.get("max_attempts", spec.max_attempts),
                    priority=raw.get("priority", 0),
                    headers=raw.get("headers"),
                    available_at=raw.get("available_at"),
                    exchange=raw.get("exchange", ""),
                    routing_key=raw.get("routing_key", ""),
                )
                state_name = raw.get("state", "READY")
                queue.messages[message.id] = message
                self._by_id[message.id] = message.queue
                if state_name in {"INFLIGHT", "READY"}:
                    heapq.heappush(
                        queue.ready, (-message.priority, self._next_seq(), message.id)
                    )
                elif state_name == "DELAYED":
                    when = message.available_at or self.now()
                    queue.delayed_ids.add(message.id)
                    self._delayed.push(when, self._next_seq(), message.queue, message.id)
                elif state_name == "DEAD":
                    heapq.heappush(
                        queue.ready, (-message.priority, self._next_seq(), message.id)
                    )

    def _message_state(self, queue: DurableQueue, message_id: str) -> str:
        if message_id in queue.inflight:
            return "INFLIGHT"
        if message_id in queue.delayed_ids:
            return "DELAYED"
        if is_dlq(queue.name):
            return "DEAD"
        return "READY"

    def _retry_or_dead(self, queue: DurableQueue, message: Message, *, persist: bool) -> None:
        message.attempts += 1
        if message.attempts >= message.max_attempts:
            self._move_to_dlq(queue, message, persist=persist)
            return
        delay = retry_delay(
            message.attempts,
            mode=self.retry_backoff,
            base=self.retry_base_delay,
        )
        message.available_at = self.now() + delay
        queue.delayed_ids.add(message.id)
        self._delayed.push(message.available_at, self._next_seq(), queue.name, message.id)
        queue.stats.retried += 1
        if persist:
            self.journal.append(
                "MESSAGE_REQUEUE",
                {
                    "id": message.id,
                    "queue": queue.name,
                    "available_at": message.available_at,
                    "attempts": message.attempts,
                },
            )

    def _move_to_dlq(self, queue: DurableQueue, message: Message, *, persist: bool) -> None:
        queue.messages.pop(message.id, None)
        self._by_id.pop(message.id, None)
        queue.stats.dead += 1
        if persist:
            self.journal.append(
                "MESSAGE_DEAD",
                {"id": message.id, "queue": queue.name, "attempts": message.attempts},
                message.payload,
            )
        if not queue.spec.dead_letter:
            return
        dest_name = dlq_name(queue.name)
        dest = self.declare_queue(dest_name, dead_letter=False, persist=False)
        message.queue = dest_name
        message.available_at = None
        dest.messages[message.id] = message
        self._by_id[message.id] = dest_name
        heapq.heappush(dest.ready, (-message.priority, self._next_seq(), message.id))
        dest.dlq_ids.append(message.id)
        dest.stats.published += 1

    def _release_delayed(self, now: float) -> int:
        due = self._delayed.pop_due(now, lambda mid: True)
        n = 0
        for queue_name, message_id in due:
            queue = self._queues.get(queue_name)
            if queue is None:
                continue
            if message_id not in queue.delayed_ids:
                continue
            queue.delayed_ids.discard(message_id)
            message = queue.messages.get(message_id)
            if message is None or message_id in queue.inflight:
                continue
            message.available_at = None
            heapq.heappush(queue.ready, (-message.priority, self._next_seq(), message.id))
            n += 1
        return n

    def _timeout_inflight(self, now: float) -> int:
        n = 0
        heap = self._visibility_heap
        while heap and heap[0][0] <= now:
            deadline, _seq, queue_name, message_id = heapq.heappop(heap)
            queue = self._queues.get(queue_name)
            if queue is None:
                continue
            record = queue.inflight.get(message_id)
            if record is None or record.visibility_deadline != deadline:
                continue
            queue.inflight.pop(message_id, None)
            message = queue.messages.get(message_id)
            if message is None:
                continue
            log.info("visibility timeout message_id=%s queue=%s", message_id, queue_name)
            self._retry_or_dead(queue, message, persist=True)
            n += 1
        return n

    def _remember_ack(self, message_id: str) -> None:
        self._recent_acks[message_id] = None
        while len(self._recent_acks) > self._recent_ack_limit:
            self._recent_acks.popitem(last=False)

    def _drop_ready(self, queue: DurableQueue, message_id: str) -> None:
        if not queue.ready:
            return
        queue.ready = [item for item in queue.ready if item[2] != message_id]
        heapq.heapify(queue.ready)

    def queue_info(self, name: str) -> dict[str, Any] | None:
        queue = self._queues.get(name)
        if queue is None:
            return None
        return {
            "name": name,
            "ready": queue.ready_count(),
            "inflight": queue.inflight_count(),
            "delayed": queue.delayed_count(),
            "dlq": (
                0
                if is_dlq(name)
                else (
                    self._queues[dlq_name(name)].ready_count()
                    if dlq_name(name) in self._queues
                    else 0
                )
            ),
            "consumers": len(queue.consumer_ids),
            "published": queue.stats.published,
            "delivered": queue.stats.delivered,
            "acked": queue.stats.acked,
            "nacked": queue.stats.nacked,
            "retried": queue.stats.retried,
            "dead": queue.stats.dead,
            "visibility_timeout": queue.spec.visibility_timeout,
            "max_attempts": queue.spec.max_attempts,
            "max_length": queue.spec.max_length,
        }
