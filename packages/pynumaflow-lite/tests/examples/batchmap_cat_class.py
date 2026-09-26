import asyncio
from collections.abc import AsyncIterable

from pynumaflow_lite.batchmapper import BatchMapAsyncServer, BatchMapper, BatchResponse, Datum, Message


class SimpleBatchCat(BatchMapper):
    async def handler(self, batch: AsyncIterable[Datum]) -> list[BatchResponse]:
        responses = []
        async for d in batch:
            if d.value == b"bad world":
                responses.append(BatchResponse(d.id, Message.to_drop()))
            else:
                responses.append(BatchResponse(d.id, Message(d.value, d.keys)))
        return responses


async def main():
    await BatchMapAsyncServer(
        SimpleBatchCat(),
        sock_file="/tmp/var/run/numaflow/batchmap.sock",
        server_info_file="/tmp/var/run/numaflow/mapper-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
