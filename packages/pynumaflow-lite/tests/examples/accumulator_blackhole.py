"""
Blackhole accumulator example.

This accumulator discards every datum it receives. Instead of yielding nothing, it yields a
drop message for each datum so the watermark can progress and the tracked state can be released.
"""

import asyncio
import signal
from collections.abc import AsyncIterator

from pynumaflow_lite.accumulator import (
    Accumulator,
    AccumulatorAsyncServer,
    Datum,
    Message,
)


class Blackhole(Accumulator):
    async def handler(self, datums: AsyncIterator[Datum]) -> AsyncIterator[Message]:
        async for datum in datums:
            print(f"Dropping datum: id={datum.id}, event_time={datum.event_time}")
            yield Message.message_to_drop(datum)


async def main():
    sock_file = "/tmp/var/run/numaflow/accumulator.sock"
    server_info_file = "/tmp/var/run/numaflow/accumulator-server-info"
    server = AccumulatorAsyncServer(sock_file, server_info_file)

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        pass

    try:
        print("Starting Blackhole Accumulator Server...")
        await server.start(Blackhole)
        print("Shutting down gracefully...")
    except asyncio.CancelledError:
        server.stop()
        return


if __name__ == "__main__":
    asyncio.run(main())
