"""Minimal FastAPI publisher. Install FastAPI separately: pip install fastapi uvicorn."""

from __future__ import annotations

from contextlib import asynccontextmanager

from oneopen_broker import AsyncBroker

try:
    from fastapi import FastAPI
except ImportError as exc:  # pragma: no cover
    raise SystemExit("pip install fastapi uvicorn") from exc

broker = AsyncBroker("127.0.0.1", 6380)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await broker.connect()
    yield
    await broker.close()


app = FastAPI(lifespan=lifespan)


@app.post("/enqueue/{queue}")
async def enqueue(queue: str, body: bytes):
    message_id = await broker.publish(body, queue=queue)
    return {"id": message_id}
