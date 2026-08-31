"""Native producer/consumer example.

Start the broker: ``oneopen-broker start``
Run this: ``python examples/native/produce_consume.py``
"""

from __future__ import annotations

import asyncio

from oneopen_broker import AsyncBroker


async def main() -> None:
    async with AsyncBroker("127.0.0.1", 6380) as broker:
        await broker.publish(b"hello from native sdk", queue="demo")
        message = await broker.consume("demo")
        print("got", message.payload)
        await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
