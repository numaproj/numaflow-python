import asyncio
import signal
from collections.abc import AsyncIterable
from typing import Awaitable, Callable

from pynumaflow_lite import batchmapper
from pynumaflow_lite.batchmapper import Message


class SimpleBatchCat(batchmapper.BatchMapper):
    async def handler(self, batch: AsyncIterable[batchmapper.Datum]) -> batchmapper.BatchResponses:
        responses = batchmapper.BatchResponses()
        async for d in batch:
            resp = batchmapper.BatchResponse(d.id)
            if d.value == b"bad world":
                resp.append(Message.message_to_drop())
                continue

            resp.append(Message(d.value, d.keys))
            responses.append(resp)
        return responses


# Optional: ensure default signal handlers are in place so asyncio.run can handle them cleanly.
signal.signal(signal.SIGINT, signal.default_int_handler)
try:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
except AttributeError:
    pass


async def start(f: Callable[[AsyncIterable[batchmapper.Datum]], Awaitable[batchmapper.BatchResponses]]):
    server = batchmapper.BatchMapAsyncServer()

    # Register loop-level signal handlers so we control shutdown and avoid asyncio.run
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        pass

    try:
        await server.start(f)
        print("Shutting down gracefully...")
    except asyncio.CancelledError:
        try:
            server.stop()
        except Exception:
            pass
        return


if __name__ == "__main__":
    async_handler = SimpleBatchCat()
    asyncio.run(start(async_handler))
