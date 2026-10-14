import asyncio
from collections.abc import AsyncIterable

from pynumaflow_lite.reducer import Datum, Message, Metadata, ReduceAsyncServer


async def reduce_handler(keys: list[str], datums: AsyncIterable[Datum], md: Metadata) -> list[Message]:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = f"counter:{counter} interval_window_start:{interval_window.start} interval_window_end:{interval_window.end}"
    return [Message(msg.encode(), keys=keys)]


async def main() -> None:
    await ReduceAsyncServer(
        reduce_handler,
        sock_file="/tmp/var/run/numaflow/reduce.sock",
        server_info_file="/tmp/var/run/numaflow/reducer-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
