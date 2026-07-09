from collections.abc import AsyncIterator

from pynumaflow_lite import batchmapper
from pynumaflow_lite.batchmapper import BatchResponse, Datum, Message


async def async_handler(batch: AsyncIterator[Datum]) -> list[BatchResponse]:
    responses = []
    async for d in batch:
        resp = BatchResponse(d.id)
        if d.value == b"bad world":
            resp.append(Message.to_drop())
        else:
            resp.append(Message(d.value, keys=d.keys))
        responses.append(resp)
    return responses


if __name__ == "__main__":
    batchmapper.BatchMapAsyncServer(
        async_handler,
        sock_file="/tmp/var/run/numaflow/batchmap.sock",
        server_info_file="/tmp/var/run/numaflow/mapper-server-info",
    ).run()
