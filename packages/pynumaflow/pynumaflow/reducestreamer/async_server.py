import asyncio
import contextlib
import inspect
import sys

import aiorun
import grpc

from pynumaflow.info.server import write as info_server_write
from pynumaflow.info.types import ServerInfo, ContainerType, MINIMUM_NUMAFLOW_VERSION
from pynumaflow.proto.reducer import reduce_pb2_grpc

from pynumaflow.reducestreamer.servicer.async_servicer import AsyncReduceStreamServicer

from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    NUM_THREADS_DEFAULT,
    _LOGGER,
    REDUCE_STREAM_SOCK_PATH,
    REDUCE_STREAM_SERVER_INFO_FILE_PATH,
    MAX_NUM_THREADS,
    NUMAFLOW_GRPC_SHUTDOWN_GRACE_PERIOD_SECONDS,
)

from pynumaflow.reducestreamer._dtypes import (
    ReduceStreamCallable,
    _ReduceStreamBuilderClass,
    ReduceStreamer,
)

from pynumaflow.shared.server import NumaflowServer, check_instance


def get_handler(
    reducer_handler: ReduceStreamCallable, init_args: tuple = (), init_kwargs: dict | None = None
):
    """
    Get the correct handler type based on the arguments passed
    """
    if inspect.isfunction(reducer_handler):
        if init_args or init_kwargs:
            # if the init_args or init_kwargs are passed, then the reduce_stream_instance
            # can only be of class ReduceStreamer type
            raise TypeError("Cannot pass function handler with init args or kwargs")
        # return the function handler
        return reducer_handler
    elif not check_instance(reducer_handler, ReduceStreamer) and issubclass(
        reducer_handler, ReduceStreamer
    ):
        # if handler is type of Class ReduceStreamer, create a new instance of
        # a ReducerBuilderClass
        return _ReduceStreamBuilderClass(reducer_handler, init_args, init_kwargs)
    else:
        _LOGGER.error(
            _error_msg := f"Invalid Class Type {reducer_handler}: "
            f"Please make sure the class type is passed, and it is a subclass of ReduceStreamer"
        )
        raise TypeError(_error_msg)


class ReduceStreamAsyncServer(NumaflowServer):
    """
    Class for a new Reduce Stream Server instance.
    A new servicer instance is created and attached to the server.
    The server instance is returned.

    Args:
        reduce_stream_instance: The reducer instance to be used for
                Reduce Streaming UDF
        init_args: The arguments to be passed to the reduce_stream_handler
        init_kwargs: The keyword arguments to be passed to the
            reduce_stream_handler
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
            defaults to 4 and max capped at 16
        server_info_file: The path to the server info file
        shutdown_callback: Callable, executed after loop is stopped, before
                            cancelling any tasks.
                            Useful for graceful shutdown.

    Example invocation:
    ```py
    import os
    from collections.abc import AsyncIterable
    from pynumaflow.reducestreamer import Messages, Message, Datum, Metadata,
    ReduceStreamAsyncServer, ReduceStreamer

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
                if self.counter > 20:
                    msg = f"counter:{self.counter}"
                    await output.put(Message(str.encode(msg), keys=keys))
                    self.counter = 0
            msg = f"counter:{self.counter}"
            await output.put(Message(str.encode(msg), keys=keys))

    async def reduce_handler(
            keys: list[str],
            datums: AsyncIterable[Datum],
            output: NonBlockingIterator,
            md: Metadata,
        ):
        counter = 0
        async for _ in datums:
            counter += 1
            if counter > 20:
                msg = f"counter:{counter}"
                await output.put(Message(str.encode(msg), keys=keys))
                counter = 0
        msg = f"counter:{counter}"
        await output.put(Message(str.encode(msg), keys=keys))

    if __name__ == "__main__":
        invoke = os.getenv("INVOKE", "func_handler")
        if invoke == "class":
            # Here we are using the class instance as the reducer_instance
            # which will be used to invoke the handler function.
            # We are passing the init_args for the class instance.
            grpc_server = ReduceStreamAsyncServer(ReduceCounter, init_args=(0,))
        else:
            # Here we are using the handler function directly as the reducer_instance.
            grpc_server = ReduceStreamAsyncServer(reduce_handler)
        grpc_server.start()
    ```
    """

    def __init__(
        self,
        reduce_stream_instance: ReduceStreamCallable,
        init_args: tuple = (),
        init_kwargs: dict | None = None,
        sock_path=REDUCE_STREAM_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=REDUCE_STREAM_SERVER_INFO_FILE_PATH,
        shutdown_callback=None,
    ):
        init_kwargs = init_kwargs or {}
        self.reduce_stream_handler = get_handler(reduce_stream_instance, init_args, init_kwargs)
        self.sock_path = f"unix://{sock_path}"
        self.max_message_size = max_message_size
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.server_info_file = server_info_file
        self.shutdown_callback = shutdown_callback

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]
        # Get the servicer instance for the async server
        self.servicer = AsyncReduceStreamServicer(self.reduce_stream_handler)
        self._error: BaseException | None = None

    def start(self):
        """
        Starter function for the Async server class, need a separate caller
        so that all the async coroutines can be started from a single context
        """
        _LOGGER.info(
            "Starting Async Reduce Stream Server",
        )

        def _shutdown_handler(loop):
            _LOGGER.info("Received graceful shutdown signal, shutting down ReduceStream server")
            if self.shutdown_callback:
                self.shutdown_callback(loop)

        aiorun.run(self.aexec(), use_uvloop=True, shutdown_callback=_shutdown_handler)
        if self._error:
            _LOGGER.critical("Server exiting due to UDF error: %s", self._error)
            sys.exit(1)

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

        # The asyncio.Event must be created here (inside aexec) rather than in __init__,
        # because it must be bound to the running event loop that aiorun creates.
        shutdown_event = asyncio.Event()
        self.servicer.set_shutdown_event(shutdown_event)

        reduce_pb2_grpc.add_ReduceServicer_to_server(self.servicer, server)

        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Reducestreamer]

        await server.start()
        info_server_write(server_info=serv_info, info_file=self.server_info_file)

        _LOGGER.info(
            "Async GRPC Reduce Stream Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )

        async def _watch_for_shutdown():
            """Wait for the shutdown event and stop the server with a grace period."""
            await shutdown_event.wait()
            _LOGGER.info("Shutdown signal received, stopping server gracefully...")
            await server.stop(NUMAFLOW_GRPC_SHUTDOWN_GRACE_PERIOD_SECONDS)

        shutdown_task = asyncio.create_task(_watch_for_shutdown())
        await server.wait_for_termination()

        # Propagate error so start() can exit with a non-zero code
        self._error = self.servicer._error

        shutdown_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await shutdown_task

        _LOGGER.info("Stopping event loop...")
        asyncio.get_event_loop().stop()
        _LOGGER.info("Event loop stopped")
