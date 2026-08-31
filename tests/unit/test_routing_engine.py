from __future__ import annotations

from oneopen_broker.core.message import Message
from oneopen_broker.core.queue import QueueEngine
from oneopen_broker.core.results import EnqueueResult
from oneopen_broker.core.routing import RoutingEngine


def test_direct_routing() -> None:
    queues = QueueEngine()
    routing = RoutingEngine(queues)
    queues.declare_queue("images")
    routing.on_queue_declared("images")
    routing.declare_exchange("ex", "direct")
    routing.bind("ex", "images", "jpg")
    _mid, routed, result = routing.publish(b"x", exchange="ex", routing_key="jpg")
    assert result is EnqueueResult.OK
    assert routed == ["images"]
    msg = queues.reserve("images", "c")
    assert msg is not None
    assert msg.payload == b"x"


def test_fanout_routing() -> None:
    queues = QueueEngine()
    routing = RoutingEngine(queues)
    queues.declare_queue("a")
    queues.declare_queue("b")
    routing.on_queue_declared("a")
    routing.on_queue_declared("b")
    routing.declare_exchange("all", "fanout")
    routing.bind("all", "a")
    routing.bind("all", "b")
    _mid, routed, result = routing.publish(b"y", exchange="all", routing_key="z")
    assert result is EnqueueResult.OK
    assert set(routed) == {"a", "b"}


def test_implicit_queue_publish() -> None:
    queues = QueueEngine()
    routing = RoutingEngine(queues)
    _mid, routed, result = routing.publish(b"z", queue="gpu")
    assert result is EnqueueResult.OK
    assert routed == ["gpu"]
    assert queues.reserve("gpu", "c") is not None
    _ = Message
