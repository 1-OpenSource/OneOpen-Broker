"""Unit tests for the in-memory queue engine (no networking)."""

from __future__ import annotations

from oneopen_broker.core.consumers import ConsumerManager
from oneopen_broker.core.dlq import dlq_name
from oneopen_broker.core.message import Message
from oneopen_broker.core.queue import QueueEngine
from oneopen_broker.core.results import AckResult, EnqueueResult, NackResult
from oneopen_broker.core.retry import retry_delay


class Clock:
    def __init__(self, t: float = 1_000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def engine(**kwargs) -> tuple[QueueEngine, Clock]:
    clock = Clock()
    kwargs.setdefault("retry_base_delay", 1.0)
    return QueueEngine(clock=clock, **kwargs), clock


def test_retry_delay_exponential() -> None:
    assert retry_delay(1, mode="exponential", base=1.0) == 1.0
    assert retry_delay(2, mode="exponential", base=1.0) == 2.0
    assert retry_delay(3, mode="exponential", base=1.0) == 4.0
    assert retry_delay(1, mode="fixed", base=5.0) == 5.0


def test_declare_and_enqueue_ready() -> None:
    eng, _ = engine()
    eng.declare_queue("tasks")
    msg = Message.create("tasks", b"hello")
    assert eng.enqueue(msg) is EnqueueResult.OK
    info = eng.queue_info("tasks")
    assert info is not None
    assert info["ready"] == 1
    assert info["inflight"] == 0


def test_reserve_ack() -> None:
    eng, _ = engine()
    eng.declare_queue("tasks")
    msg = Message.create("tasks", b"hello")
    eng.enqueue(msg)
    got = eng.reserve("tasks", "c1")
    assert got is not None
    assert got.id == msg.id
    assert eng.queue_info("tasks")["inflight"] == 1
    assert eng.ack(got.id) is AckResult.OK
    assert eng.queue_info("tasks")["ready"] == 0
    assert eng.queue_info("tasks")["inflight"] == 0
    assert eng.ack(got.id) is AckResult.ALREADY_ACKED
    assert eng.ack("missing") is AckResult.NOT_FOUND


def test_nack_requeue_then_retry_delay() -> None:
    eng, clock = engine(max_attempts=3)
    eng.declare_queue("tasks")
    msg = Message.create("tasks", b"x", max_attempts=3)
    eng.enqueue(msg)
    got = eng.reserve("tasks", "c1")
    assert got is not None
    assert eng.nack(got.id, requeue=True) is NackResult.OK
    assert eng.queue_info("tasks")["delayed"] == 1
    assert eng.reserve("tasks", "c1") is None
    clock.advance(1.0)
    eng.tick()
    again = eng.reserve("tasks", "c1")
    assert again is not None
    assert again.attempts == 1


def test_nack_without_requeue_goes_to_dlq() -> None:
    eng, _ = engine()
    eng.declare_queue("tasks")
    msg = Message.create("tasks", b"x")
    eng.enqueue(msg)
    got = eng.reserve("tasks", "c1")
    assert got is not None
    eng.nack(got.id, requeue=False)
    assert eng.queue_info("tasks")["ready"] == 0
    dlq = eng.queue_info(dlq_name("tasks"))
    assert dlq is not None
    assert dlq["ready"] == 1


def test_visibility_timeout_retries() -> None:
    eng, clock = engine(visibility_timeout=10.0, max_attempts=3)
    eng.declare_queue("tasks", visibility_timeout=10.0)
    eng.enqueue(Message.create("tasks", b"x", max_attempts=3))
    got = eng.reserve("tasks", "c1")
    assert got is not None
    clock.advance(10.0)
    assert eng.tick() == 1
    assert eng.queue_info("tasks")["inflight"] == 0
    assert eng.queue_info("tasks")["delayed"] == 1
    clock.advance(1.0)
    eng.tick()
    again = eng.reserve("tasks", "c2")
    assert again is not None
    assert again.id == got.id
    assert again.attempts == 1


def test_exhausted_retries_go_to_dlq() -> None:
    eng, clock = engine(visibility_timeout=1.0, max_attempts=2, retry_base_delay=0.0)
    eng.declare_queue("tasks", visibility_timeout=1.0, max_attempts=2)
    eng.enqueue(Message.create("tasks", b"x", max_attempts=2))
    for _ in range(2):
        got = eng.reserve("tasks", "c1")
        assert got is not None
        clock.advance(1.0)
        eng.tick()
        clock.advance(0.0)
        eng.tick()
    assert eng.queue_info("tasks")["ready"] == 0
    assert eng.queue_info(dlq_name("tasks"))["ready"] == 1


def test_priority_order() -> None:
    eng, _ = engine()
    eng.declare_queue("tasks")
    low = Message.create("tasks", b"low", priority=0)
    high = Message.create("tasks", b"high", priority=9)
    eng.enqueue(low)
    eng.enqueue(high)
    first = eng.reserve("tasks", "c1")
    assert first is not None
    assert first.payload == b"high"


def test_queue_full() -> None:
    eng, _ = engine(max_length=1)
    eng.declare_queue("tasks", max_length=1)
    assert eng.enqueue(Message.create("tasks", b"a")) is EnqueueResult.OK
    assert eng.enqueue(Message.create("tasks", b"b")) is EnqueueResult.QUEUE_FULL


def test_consumer_disconnect_recovers_inflight() -> None:
    eng, _ = engine(retry_base_delay=0.0)
    mgr = ConsumerManager(eng)
    eng.declare_queue("tasks")
    eng.enqueue(Message.create("tasks", b"x"))
    mgr.register("c1", "conn1", ["tasks"], prefetch_count=1)
    delivered = mgr.try_reserve("tasks")
    assert delivered is not None
    mgr.cancel("c1")
    eng.tick()
    got = eng.reserve("tasks", "c2")
    assert got is not None
    assert got.payload == b"x"


def test_prefetch_blocks_extra_delivery() -> None:
    eng, _ = engine()
    mgr = ConsumerManager(eng)
    eng.declare_queue("tasks")
    eng.enqueue(Message.create("tasks", b"a"))
    eng.enqueue(Message.create("tasks", b"b"))
    mgr.register("c1", "conn1", ["tasks"], prefetch_count=1)
    first = mgr.try_reserve("tasks")
    second = mgr.try_reserve("tasks")
    assert first is not None
    assert second is None
    mgr.on_ack("c1")
    eng.ack(first[1].id)
    third = mgr.try_reserve("tasks")
    assert third is not None


def test_fair_round_robin() -> None:
    eng, _ = engine()
    mgr = ConsumerManager(eng)
    eng.declare_queue("tasks")
    for i in range(4):
        eng.enqueue(Message.create("tasks", bytes([i])))
    mgr.register("c1", "conn1", ["tasks"], prefetch_count=10)
    mgr.register("c2", "conn2", ["tasks"], prefetch_count=10)
    owners = []
    while True:
        item = mgr.try_reserve("tasks")
        if item is None:
            break
        owners.append(item[0].consumer_id)
    assert owners.count("c1") == 2
    assert owners.count("c2") == 2


def test_duplicate_nack_unknown() -> None:
    eng, _ = engine()
    eng.declare_queue("tasks")
    eng.enqueue(Message.create("tasks", b"x"))
    got = eng.reserve("tasks", "c1")
    assert got is not None
    assert eng.nack(got.id, requeue=True) is NackResult.OK
    assert eng.nack(got.id, requeue=True) is NackResult.UNKNOWN_DELIVERY


def test_dlq_requeue() -> None:
    eng, _ = engine()
    eng.declare_queue("tasks")
    eng.enqueue(Message.create("tasks", b"x"))
    got = eng.reserve("tasks", "c1")
    assert got is not None
    eng.nack(got.id, requeue=False)
    assert eng.dlq_requeue("tasks", got.id) is True
    again = eng.reserve("tasks", "c1")
    assert again is not None
    assert again.payload == b"x"
    assert again.attempts == 0


def test_enqueue_unknown_queue() -> None:
    eng, _ = engine()
    assert eng.enqueue(Message.create("nope", b"x")) is EnqueueResult.NOT_FOUND
