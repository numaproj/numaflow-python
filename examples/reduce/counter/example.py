import os
from collections.abc import AsyncIterable

from pynumaflow.reducer import Messages, Message, Datum, Metadata, ReduceAsyncServer, Reducer


class Example(Reducer):
    def __init__(self):
        self.counter = 0

    async def handler(
        self, keys: list[str], datums: AsyncIterable[Datum], md: Metadata
    ) -> Messages:
        interval_window = md.interval_window
        self.counter = 0
        async for _ in datums:
            self.counter += 1
        msg = (
            f"counter:{self.counter} interval_window_start:{interval_window.start} "
            f"interval_window_end:{interval_window.end}"
        )
        return Messages(Message(str.encode(msg), keys=keys))


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
    invoke = os.getenv("INVOKE", "handler")
    if invoke == "class":
        grpc_server = ReduceAsyncServer(Example())
    else:
        grpc_server = ReduceAsyncServer(reduce_handler)
    grpc_server.start()
