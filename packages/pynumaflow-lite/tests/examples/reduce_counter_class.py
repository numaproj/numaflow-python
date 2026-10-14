import asyncio
import functools
from collections.abc import AsyncIterator

from pynumaflow_lite.reducer import Datum, Message, Metadata, ReduceAsyncServer, Reducer


class ReduceCounter(Reducer):
    def __init__(self, initial: int = 0) -> None:
        self.initial = initial

    async def handler(self, keys: list[str], datums: AsyncIterator[Datum], md: Metadata) -> list[Message]:
        counter = self.initial
        async for _ in datums:
            counter += 1
        iw = md.interval_window
        msg = f"counter:{counter} interval_window_start:{iw.start} interval_window_end:{iw.end}"
        return [Message(msg.encode(), keys=keys)]


async def main() -> None:
    await ReduceAsyncServer(
        functools.partial(ReduceCounter, initial=0),
        sock_file="/tmp/var/run/numaflow/reduce.sock",
        server_info_file="/tmp/var/run/numaflow/reducer-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
