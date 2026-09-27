import asyncio
from collections.abc import AsyncIterator

from pynumaflow_lite.mapstreamer import Datum, MapStreamAsyncServer, Message


async def async_handler(datum: Datum) -> AsyncIterator[Message]:
    """
    A handler that splits the input datum value into multiple strings by `,` separator and
    emits them as a stream.
    """
    if not datum.value:
        yield Message.to_drop()
        return
    for s in datum.value.decode("utf-8").split(","):
        yield Message(s.encode(), keys=datum.keys)


async def main():
    await MapStreamAsyncServer(
        handler=async_handler,
        sock_file="/tmp/var/run/numaflow/mapstream.sock",
        server_info_file="/tmp/var/run/numaflow/mapper-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
