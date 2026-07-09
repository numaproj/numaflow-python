from collections.abc import AsyncIterator

from pynumaflow_lite import batchmapper
from pynumaflow_lite.batchmapper import BatchMapper, BatchResponse, Datum, Message


class SimpleBatchCat(BatchMapper):
    async def handler(self, batch: AsyncIterator[Datum]) -> list[BatchResponse]:
        return [
            BatchResponse(
                d.id,
                [Message.to_drop()] if d.value == b"bad world" else [Message(d.value, keys=d.keys)],
            )
            async for d in batch
        ]


if __name__ == "__main__":
    batchmapper.BatchMapAsyncServer(
        SimpleBatchCat(),
        sock_file="/tmp/var/run/numaflow/batchmap.sock",
        server_info_file="/tmp/var/run/numaflow/mapper-server-info",
    ).run()
