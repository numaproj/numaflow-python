import asyncio
from collections.abc import AsyncIterator

from pynumaflow_lite.mapstreamer import Datum, MapStreamAsyncServer, MapStreamer, Message


class SimpleStreamCat(MapStreamer):
    async def handler(self, datum: Datum) -> AsyncIterator[Message]:
        if not datum.value:
            yield Message.to_drop()
            return
        # Forward the incoming user metadata and echo the system metadata
        user_metadata = {
            **datum.user_metadata,
            "system_echo": {
                f"{group}.{key}": value
                for group, key_values in datum.system_metadata.items()
                for key, value in key_values.items()
            },
        }
        for s in datum.value.decode("utf-8").split(","):
            yield Message(s.encode(), datum.keys, user_metadata=user_metadata)


async def main():
    await MapStreamAsyncServer(
        SimpleStreamCat(),
        sock_file="/tmp/var/run/numaflow/mapstream.sock",
        server_info_file="/tmp/var/run/numaflow/mapper-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
