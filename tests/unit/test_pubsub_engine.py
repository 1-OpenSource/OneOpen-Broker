from __future__ import annotations

from oneopen_broker.protocol.codec import event_frame
from oneopen_broker.pubsub.channels import PubSubEngine


def test_slow_subscriber_isolated() -> None:
    ps = PubSubEngine()

    def fast(conn: str, payload: bytes) -> bool:
        return True

    def slow(conn: str, payload: bytes) -> bool:
        return False

    ps.subscribe("c1", "ch", lambda _b: True)
    ps.subscribe("c2", "ch", lambda _b: True)

    def send_event(conn_id: str, body: bytes) -> bool:
        if conn_id == "c2":
            return slow(conn_id, body)
        return fast(conn_id, body)

    result = ps.publish("ch", b"x", send_event)
    assert result["delivered"] == 1
    assert result["slow"] == ["c2"]
    ps.disconnect("c2")
    result2 = ps.publish("ch", b"y", send_event)
    assert result2["delivered"] == 1
    assert result2["slow"] == []
    _ = event_frame
