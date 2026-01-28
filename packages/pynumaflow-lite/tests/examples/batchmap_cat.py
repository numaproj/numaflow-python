import asyncio
import collections.abc
import signal
from typing import Awaitable, Callable

from pynumaflow_lite import batchmapper


async def async_handler(batch: collections.abc.AsyncIterator[batchmapper.Datum]) -> batchmapper.BatchResponses:
    responses = batchmapper.BatchResponses()
    async for d in batch:
        resp = batchmapper.BatchResponse.from_id(d.id)
        if d.value == b"bad world":
            resp.append(batchmapper.Message.message_to_drop())
            continue

        resp.append(batchmapper.Message(d.value, d.keys))
        responses.append(resp)
    return responses


async def start(f: Callable[[collections.abc.AsyncIterator[batchmapper.Datum]], Awaitable[batchmapper.BatchResponses]]):
    sock_file = "/tmp/var/run/numaflow/batchmap.sock"
    server_info_file = "/tmp/var/run/numaflow/mapper-server-info"
    server = batchmapper.BatchMapAsyncServer(sock_file, server_info_file)

    # Register loop-level signal handlers to request graceful shutdown
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
    asyncio.run(start(async_handler))
