import asyncio
import signal
from collections.abc import AsyncIterable

from pynumaflow_lite.batchmapper import Datum, BatchResponse, Message, BatchMapAsyncServer


async def async_handler(
    batch: AsyncIterable[Datum],
) -> list[BatchResponse]:
    responses = []
    async for d in batch:
        if d.value == b"bad world":
            responses.append(BatchResponse(d.id, Message.to_drop()))
        else:
            responses.append(BatchResponse(d.id, Message(d.value, keys=d.keys)))
    return responses


async def main():
    await BatchMapAsyncServer(
        handler=async_handler,
        sock_file = "/tmp/var/run/numaflow/batchmap.sock",
        server_info_file = "/tmp/var/run/numaflow/mapper-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
