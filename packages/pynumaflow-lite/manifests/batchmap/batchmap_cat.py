from collections.abc import AsyncIterator

from pynumaflow_lite import batchmapper
from pynumaflow_lite.batchmapper import BatchResponse, Datum, Message


class SimpleBatchCat:
    async def handler(self, batch: AsyncIterator[Datum]) -> list[BatchResponse]:
        return [
            BatchResponse(
                d.id,
                [Message.to_drop()] if d.value == b"bad world" else [Message(d.value, keys=d.keys)],
            )
            async for d in batch
        ]


if __name__ == "__main__":
    batch_mapper_obj = SimpleBatchCat()
    batchmapper.BatchMapAsyncServer(batch_mapper_obj.handler).run()
