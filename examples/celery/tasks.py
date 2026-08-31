"""Celery demo app using OneOpen Broker.

Terminal 1: oneopen-broker start
Terminal 2: celery -A examples.celery.tasks worker --loglevel=info --concurrency=4
Terminal 3: python -c "from examples.celery.tasks import add; print(add.delay(2, 3).get(timeout=10))"
"""

from __future__ import annotations

import oneopen_broker

oneopen_broker.register_kombu()

from celery import Celery

app = Celery(
    "demo",
    broker="oneopen://127.0.0.1:6380",
    backend="rpc://",
)


@app.task
def add(x: int, y: int) -> int:
    return x + y
