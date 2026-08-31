"""Broker process lifecycle: recover, listen, shutdown."""

from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

from oneopen_broker.broker.config import BrokerConfig
from oneopen_broker.broker.runtime import BrokerRuntime
from oneopen_broker.broker.server import BrokerServer
from oneopen_broker.persistence.aof import AOFWriter
from oneopen_broker.persistence.recovery import recover_into
from oneopen_broker.persistence.snapshot import write_snapshot

log = logging.getLogger("oneopen_broker.broker.lifecycle")


class Broker:
    def __init__(self, config: BrokerConfig) -> None:
        self.config = config
        data_dir = Path(config.persistence.directory)
        self.aof = AOFWriter(data_dir / "appendonly.aof", fsync=config.persistence.fsync)
        self.runtime = BrokerRuntime(config, journal=self.aof)
        self.server = BrokerServer(config, self.runtime)
        self._snapshot_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._closed = False

    async def start(self) -> None:
        last_seq = recover_into(self.runtime, self.config.persistence.directory)
        self.aof.attach_existing_seq(last_seq)
        await self.aof.start()
        await self.server.start()
        if self.config.persistence.snapshot_interval > 0:
            self._snapshot_task = asyncio.create_task(self._snapshot_loop())
        log.info("broker ready")

    async def wait(self) -> None:
        await self._stop.wait()

    async def shutdown(self) -> None:
        if self._closed:
            self._stop.set()
            return
        self._closed = True
        log.info("broker shutting down")
        await self.server.stop()
        if self._snapshot_task is not None:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass
            self._snapshot_task = None
        await self.aof.flush_and_stop()
        if self.config.persistence.snapshot_on_shutdown:
            self._write_snapshot()
        self._stop.set()
        log.info("broker stopped")

    def request_shutdown(self) -> None:
        self._stop.set()

    async def _snapshot_loop(self) -> None:
        interval = self.config.persistence.snapshot_interval
        while True:
            await asyncio.sleep(interval)
            try:
                self._write_snapshot()
            except Exception:
                log.exception("snapshot failed")

    def _write_snapshot(self) -> None:
        path = Path(self.config.persistence.directory) / "snapshot.dat"
        write_snapshot(path, self.aof.seq, self.runtime.snapshot())


async def run_broker(config: BrokerConfig) -> None:
    broker = Broker(config)
    loop = asyncio.get_running_loop()

    def _signal(*_args: object) -> None:
        log.info("signal received, shutting down")
        broker.request_shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, lambda *_: _signal())

    await broker.start()
    try:
        await broker.wait()
    finally:
        await broker.shutdown()
