import aiorun
from collections.abc import AsyncIterable

from pynumaflow.function import Messages, Message, Datum, Metadata, AsyncServer


async def reduce_handler(keys: list[str], datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    return Messages(Message(str.encode(msg), keys=keys))


if __name__ == "__main__":
    grpc_server = AsyncServer(reduce_handler=reduce_handler)
    aiorun.run(grpc_server.start())
