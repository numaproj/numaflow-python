import os
from collections.abc import AsyncIterable

from pynumaflow.reducestreamer import (
    Message,
    Datum,
    Metadata,
    ReduceStreamAsyncServer,
    ReduceStreamer,
)
from pynumaflow.shared.asynciter import NonBlockingIterator


class ReduceCounter(ReduceStreamer):
    def __init__(self, counter):
        self.counter = counter

    async def handler(
        self,
        keys: list[str],
        datums: AsyncIterable[Datum],
        output: NonBlockingIterator,
        md: Metadata,
    ):
        async for _ in datums:
            self.counter += 1
            if self.counter > 10:
                msg = f"counter:{self.counter}"
                # NOTE: this is returning results because we have seen all the data
                # use this only if you really need this feature because your next vertex
                # will get both early result and final results and it should be able to
                # handle both the scenarios.
                await output.put(Message(str.encode(msg), keys=keys))
                self.counter = 0
        msg = f"counter:{self.counter}"
        await output.put(Message(str.encode(msg), keys=keys))


if __name__ == "__main__":
    invoke = os.getenv("INVOKE", "class")
    if invoke == "class":
        # Here we are using the class instance as the reducer_instance
        # which will be used to invoke the handler function.
        # We are passing the init_args for the class instance.
        grpc_server = ReduceStreamAsyncServer(ReduceCounter, init_args=(0,))
    grpc_server.start()
