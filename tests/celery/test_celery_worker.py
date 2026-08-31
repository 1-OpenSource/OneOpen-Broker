from __future__ import annotations

import pytest

pytestmark = pytest.mark.celery

# In-process Celery start_worker is environment-sensitive (pool/pidbox).
# Kombu drain_events + produce/consume cover the worker consume path.
# Run a real worker with: celery -A examples.celery.tasks worker --concurrency=1


def test_celery_example_imports() -> None:
    pytest.importorskip("celery")
    from oneopen_broker.kombu_transport import register_transport

    register_transport()
    import examples.celery.tasks as tasks

    assert tasks.app.conf.broker_url.startswith("oneopen://")
