import asyncio
from collections.abc import AsyncIterator

from pynumaflow_lite.reducestreamer import Datum, Message, Metadata, ReduceStreamAsyncServer


async def reducestream_handler(keys: list[str], datums: AsyncIterator[Datum], md: Metadata) -> AsyncIterator[Message]:
    iw = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
        msg = f"counter:{counter} interval_window_start:{iw.start} interval_window_end:{iw.end}"
        yield Message(msg.encode(), keys=keys)


async def main() -> None:
    await ReduceStreamAsyncServer(
        reducestream_handler,
        sock_file="/tmp/var/run/numaflow/reducestream.sock",
        server_info_file="/tmp/var/run/numaflow/reducestreamer-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
