import inspect
from typing import Optional

import aiorun
import grpc

from pynumaflow.accumulator.servicer.async_servicer import AsyncAccumulatorServicer
from pynumaflow.info.types import ServerInfo, ContainerType, MINIMUM_NUMAFLOW_VERSION
from pynumaflow.proto.accumulator import accumulator_pb2_grpc


from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    NUM_THREADS_DEFAULT,
    _LOGGER,
    MAX_NUM_THREADS,
    ACCUMULATOR_SOCK_PATH,
    ACCUMULATOR_SERVER_INFO_FILE_PATH,
)

from pynumaflow.accumulator._dtypes import (
    AccumulatorStreamCallable,
    _AccumulatorBuilderClass,
    Accumulator,
)

from pynumaflow.shared.server import NumaflowServer, check_instance, start_async_server


def get_handler(
    accumulator_handler: AccumulatorStreamCallable,
    init_args: tuple = (),
    init_kwargs: Optional[dict] = None,
):
    """
    Get the correct handler type based on the arguments passed
    """
    if inspect.isfunction(accumulator_handler):
        if init_args or init_kwargs:
            # if the init_args or init_kwargs are passed, then the accumulator_instance
            # can only be of class Accumulator type
            raise TypeError("Cannot pass function handler with init args or kwargs")
        # return the function handler
        return accumulator_handler
    elif not check_instance(accumulator_handler, Accumulator) and issubclass(
        accumulator_handler, Accumulator
    ):
        # if handler is type of Class Accumulator, create a new instance of
        # a AccumulatorBuilderClass
        return _AccumulatorBuilderClass(accumulator_handler, init_args, init_kwargs)
    else:
        _LOGGER.error(
            _error_msg := f"Invalid Class Type {accumulator_handler}: "
            f"Please make sure the class type is passed, and it is a subclass of Accumulator"
        )
        raise TypeError(_error_msg)


class AccumulatorAsyncServer(NumaflowServer):
    """
    Class for a new Accumulator Server instance.
    A new servicer instance is created and attached to the server.
    The server instance is returned.

    Args:
        accumulator_instance: The accumulator instance to be used for
                Accumulator UDF
        init_args: The arguments to be passed to the accumulator_handler
        init_kwargs: The keyword arguments to be passed to the
            accumulator_handler
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
        defaults to 4 and max capped at 16
        server_info_file: The path to the server info file

    Example invocation:
    ```py
    import os
    from collections.abc import AsyncIterable
    from datetime import datetime

    from pynumaflow.accumulator import Accumulator, AccumulatorAsyncServer
    from pynumaflow.accumulator import Message, Datum
    from pynumaflow.shared.asynciter import NonBlockingIterator

    class StreamSorter(Accumulator):
        def __init__(self, counter):
            self.latest_wm = datetime.fromtimestamp(-1)
            self.sorted_buffer: list[Datum] = []

        async def handler(
            self,
            datums: AsyncIterable[Datum],
            output: NonBlockingIterator,
        ):
            async for _ in datums:
                # Process the datums and send output
                if datum.watermark and datum.watermark > self.latest_wm:
                    self.latest_wm = datum.watermark
                    await self.flush_buffer(output)

                self.insert_sorted(datum)

        def insert_sorted(self, datum: Datum):
            # Binary insert to keep sorted buffer in order
            left, right = 0, len(self.sorted_buffer)
            while left < right:
                mid = (left + right) // 2
                if self.sorted_buffer[mid].event_time > datum.event_time:
                    right = mid
                else:
                    left = mid + 1
            self.sorted_buffer.insert(left, datum)

        async def flush_buffer(self, output: NonBlockingIterator):
            i = 0
            for datum in self.sorted_buffer:
                if datum.event_time > self.latest_wm:
                    break
                await output.put(Message.from_datum(datum))
                i += 1
            # Remove flushed items
            self.sorted_buffer = self.sorted_buffer[i:]


    if __name__ == "__main__":
        grpc_server = AccumulatorAsyncServer(StreamSorter)
        grpc_server.start()
    ```
    """

    def __init__(
        self,
        accumulator_instance: AccumulatorStreamCallable,
        init_args: tuple = (),
        init_kwargs: Optional[dict] = None,
        sock_path=ACCUMULATOR_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=ACCUMULATOR_SERVER_INFO_FILE_PATH,
    ):
        init_kwargs = init_kwargs or {}
        self.accumulator_handler = get_handler(accumulator_instance, init_args, init_kwargs)
        self.sock_path = f"unix://{sock_path}"
        self.max_message_size = max_message_size
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.server_info_file = server_info_file

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]
        # Get the servicer instance for the async server
        self.servicer = AsyncAccumulatorServicer(self.accumulator_handler)

    def start(self):
        """
        Starter function for the Async server class, need a separate caller
        so that all the async coroutines can be started from a single context
        """
        _LOGGER.info(
            "Starting Async Accumulator Server",
        )
        aiorun.run(self.aexec(), use_uvloop=True)

    async def aexec(self):
        """
        Starts the Async gRPC server on the given UNIX socket with
        given max threads.
        """
        # As the server is async, we need to create a new server instance in the
        # same thread as the event loop so that all the async calls are made in the
        # same context
        # Create a new async server instance and add the servicer to it
        server = grpc.aio.server(options=self._server_options)
        server.add_insecure_port(self.sock_path)
        accumulator_pb2_grpc.add_AccumulatorServicer_to_server(self.servicer, server)

        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Accumulator]
        await start_async_server(
            server_async=server,
            sock_path=self.sock_path,
            max_threads=self.max_threads,
            cleanup_coroutines=list(),
            server_info_file=self.server_info_file,
            server_info=serv_info,
        )
