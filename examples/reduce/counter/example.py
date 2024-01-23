import os
from collections.abc import AsyncIterable

from pynumaflow.reducer import Messages, Message, Datum, Metadata, ReduceAsyncServer, Reducer


class Example(Reducer):
    def __init__(self, counter):
        self.counter = counter

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
        # Here we are using the class instance as the reducer_instance
        # which will be used to invoke the handler function.
        # We are passing the init_args for the class instance.
        grpc_server = ReduceAsyncServer(Example, init_args=(0,))
    else:
        # Here we are using the handler function directly as the reducer_instance.
        grpc_server = ReduceAsyncServer(reduce_handler)
    grpc_server.start()
