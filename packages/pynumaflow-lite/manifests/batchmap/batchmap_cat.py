import asyncio
from collections.abc import AsyncIterable

from pynumaflow_lite.batchmapper import BatchResponse, Message, Datum, BatchMapAsyncServer, BatchMapper


class SimpleBatchCat(BatchMapper):
    async def handler(self, batch: AsyncIterable[Datum]) -> list[BatchResponse]:
        responses = []
        async for datum in batch:
            print(datum)
            if datum.value == b"bad world":
                responses.append(BatchResponse(datum.id, Message.to_drop()))
                continue

            responses.append(BatchResponse(datum.id, Message(datum.value, keys=datum.keys)))
        return responses

async def main() -> None:
    print("Starting BatchMap server")
    # `serve` returns when SIGINT or SIGTERM arrives.
    await BatchMapAsyncServer(SimpleBatchCat()).serve()
    print("BatchMap server stopped")


if __name__ == "__main__":
    asyncio.run(main())
