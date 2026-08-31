"""Ephemeral pub/sub example."""

from __future__ import annotations

import asyncio

from oneopen_broker import AsyncBroker


async def subscriber() -> None:
    async with AsyncBroker("127.0.0.1", 6380) as broker:
        async for event in broker.subscribe("events"):
            print("event", event.payload)
            break


async def publisher() -> None:
    await asyncio.sleep(0.2)
    async with AsyncBroker("127.0.0.1", 6380) as broker:
        n = await broker.publish_channel("events", b"hello subscribers")
        print("delivered to", n)


async def main() -> None:
    await asyncio.gather(subscriber(), publisher())


if __name__ == "__main__":
    asyncio.run(main())
