import asyncio
from collections.abc import AsyncIterator

from pynumaflow_lite.batchmapper import BatchMapAsyncServer, BatchResponse, Datum, Message


async def async_handler(
    batch: AsyncIterator[Datum],
) -> list[BatchResponse]:
    responses = []
    async for d in batch:
        if d.value == b"bad world":
            responses.append(BatchResponse(d.id, Message.to_drop()))
        else:
            # Forward the incoming user metadata and echo the system metadata
            user_metadata = {
                **d.user_metadata,
                "system_echo": {
                    f"{group}.{key}": value
                    for group, key_values in d.system_metadata.items()
                    for key, value in key_values.items()
                },
            }
            responses.append(BatchResponse(d.id, Message(d.value, keys=d.keys, user_metadata=user_metadata)))
    return responses


async def main():
    await BatchMapAsyncServer(
        handler=async_handler,
        sock_file="/tmp/var/run/numaflow/batchmap.sock",
        server_info_file="/tmp/var/run/numaflow/mapper-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
