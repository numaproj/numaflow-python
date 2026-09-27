import asyncio
from collections.abc import AsyncIterator

from pynumaflow_lite.mapstreamer import Datum, MapStreamAsyncServer, MapStreamer, Message


class SimpleStreamCat(MapStreamer):
    async def handler(self, datum: Datum) -> AsyncIterator[Message]:
        if not datum.value:
            yield Message.to_drop()
            return
        for s in datum.value.decode("utf-8").split(","):
            yield Message(s.encode(), keys=datum.keys)


async def main() -> None:
    print("Starting MapStream server")
    # `serve` returns when SIGINT or SIGTERM arrives.
    await MapStreamAsyncServer(SimpleStreamCat()).serve()
    print("MapStream server stopped")


if __name__ == "__main__":
    asyncio.run(main())
