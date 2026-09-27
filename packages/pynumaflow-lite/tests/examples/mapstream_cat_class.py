import asyncio
from collections.abc import AsyncIterator

from pynumaflow_lite.mapstreamer import Datum, MapStreamAsyncServer, MapStreamer, Message


class SimpleStreamCat(MapStreamer):
    async def handler(self, datum: Datum) -> AsyncIterator[Message]:
        if not datum.value:
            yield Message.to_drop()
            return
        for s in datum.value.decode("utf-8").split(","):
            yield Message(s.encode(), datum.keys)


async def main():
    await MapStreamAsyncServer(
        SimpleStreamCat(),
        sock_file="/tmp/var/run/numaflow/mapstream.sock",
        server_info_file="/tmp/var/run/numaflow/mapper-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
