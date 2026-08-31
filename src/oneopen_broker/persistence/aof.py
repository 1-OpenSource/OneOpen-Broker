"""Batched AOF writer. Disk I/O stays off the socket handler except for queueing."""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

from oneopen_broker.persistence.records import encode_record

log = logging.getLogger("oneopen_broker.persistence.aof")


class AOFWriter:
    def __init__(self, path: str | Path, fsync: str = "everysec") -> None:
        self.path = Path(path)
        self.fsync = fsync
        self.seq = 0
        self._queue: asyncio.Queue[tuple[int, bytes, asyncio.Future[None] | None]] = (
            asyncio.Queue()
        )
        self._durable_seq = 0
        self._waiters: dict[int, asyncio.Future[None]] = {}
        self._fp: Any = None
        self._task: asyncio.Task[None] | None = None
        self._last_fsync = 0.0
        self._closed = False

    def attach_existing_seq(self, seq: int) -> None:
        self.seq = seq
        self._durable_seq = seq

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = open(self.path, "ab", buffering=0)

    def append(self, op: str, meta: dict[str, Any], payload: bytes = b"") -> int:
        if self._closed:
            return self.seq
        self.seq += 1
        raw = encode_record(self.seq, op, meta, payload)
        fut: asyncio.Future[None] | None = None
        if self.fsync == "always":
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            self._waiters[self.seq] = fut
        self._queue.put_nowait((self.seq, raw, fut))
        return self.seq

    async def wait_durable(self, seq: int) -> None:
        if self.fsync != "always":
            return
        fut = self._waiters.get(seq)
        if fut is not None:
            await fut
            self._waiters.pop(seq, None)

    async def start(self) -> None:
        if self._fp is None:
            self.open()
        self._task = asyncio.create_task(self._run(), name="aof-writer")

    async def flush_and_stop(self) -> None:
        self._closed = True
        await self._queue.put((0, b"", None))
        if self._task is not None:
            await self._task
        if self._fp is not None:
            self._fp.flush()
            os.fsync(self._fp.fileno())
            self._fp.close()
            self._fp = None

    async def _run(self) -> None:
        assert self._fp is not None
        loop = asyncio.get_running_loop()
        while True:
            seq, raw, fut = await self._queue.get()
            if seq == 0 and not raw:
                break
            batch = [(seq, raw, fut)]
            while not self._queue.empty():
                item = self._queue.get_nowait()
                if item[0] == 0 and not item[1]:
                    self._queue.put_nowait(item)
                    break
                batch.append(item)
            blob = b"".join(item[1] for item in batch)
            await loop.run_in_executor(None, self._write_bytes, blob)
            last_seq = batch[-1][0]
            self._durable_seq = last_seq
            for _seq, _raw, waiter in batch:
                if waiter is not None and not waiter.done():
                    waiter.set_result(None)

    def _write_bytes(self, blob: bytes) -> None:
        assert self._fp is not None
        self._fp.write(blob)
        now = time_monotonic()
        if self.fsync == "always":
            self._fp.flush()
            os.fsync(self._fp.fileno())
            self._last_fsync = now
        elif self.fsync == "everysec":
            if now - self._last_fsync >= 1.0:
                self._fp.flush()
                os.fsync(self._fp.fileno())
                self._last_fsync = now


def time_monotonic() -> float:
    return __import__("time").monotonic()
