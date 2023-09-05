import aiorun
from collections.abc import Iterator

from pynumaflow.reduce import (
    Messages,
    Message,
    Datum,
    Metadata,
    AsyncReducer,
)


async def my_handler(keys: list[str], datums: Iterator[Datum], md: Metadata) -> Messages:
    # count the number of events
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1

    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    return Messages(Message(keys=keys, value=str.encode(msg)))


if __name__ == "__main__":
    grpc_server = AsyncReducer(reduce_handler=my_handler)
    aiorun.run(grpc_server.start())
