import logging
import os
from collections.abc import AsyncIterable

from pynumaflow import setup_logging
from pynumaflow.reducestreamer import (
    Message,
    Datum,
    Metadata,
    ReduceStreamAsyncServer,
    ReduceStreamer,
)
from pynumaflow.reducestreamer.servicer.asynciter import NonBlockingIterator

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)


class ReduceSum(ReduceStreamer):
    def __init__(self, counter):
        self.counter = counter

    async def handler(
        self,
        keys: list[str],
        datums: AsyncIterable[Datum],
        output: NonBlockingIterator,
        md: Metadata,
    ):
        async for msg in datums:
            val = int(msg.value)
            self.counter += val
            if self.counter >= 30:
                msg = f"{self.counter}"
                await output.put(Message(str.encode(msg), keys=keys))
                self.counter = 0
        msg = f"{self.counter}"
        await output.put(Message(str.encode(msg), keys=keys))


if __name__ == "__main__":
    invoke = os.getenv("INVOKE", "class")
    if invoke == "class":
        # Here we are using the class instance as the reducer_instance
        # which will be used to invoke the handler function.
        # We are passing the init_args for the class instance.
        grpc_server = ReduceStreamAsyncServer(ReduceSum, init_args=(0,))
    grpc_server.start()
