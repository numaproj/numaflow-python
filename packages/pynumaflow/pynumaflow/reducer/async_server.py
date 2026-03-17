import asyncio
import contextlib
import inspect
import sys

import aiorun
import grpc

from pynumaflow.info.server import write as info_server_write
from pynumaflow.info.types import ServerInfo, MINIMUM_NUMAFLOW_VERSION, ContainerType
from pynumaflow.proto.reducer import reduce_pb2_grpc

from pynumaflow.reducer.servicer.async_servicer import AsyncReduceServicer

from pynumaflow._constants import (
    REDUCE_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    NUM_THREADS_DEFAULT,
    _LOGGER,
    REDUCE_SERVER_INFO_FILE_PATH,
    MAX_NUM_THREADS,
    NUMAFLOW_GRPC_SHUTDOWN_GRACE_PERIOD_SECONDS,
)

from pynumaflow.reducer._dtypes import (
    ReduceCallable,
    _ReduceBuilderClass,
    Reducer,
)

from pynumaflow.shared.server import NumaflowServer, check_instance


def get_handler(
    reducer_handler: ReduceCallable, init_args: tuple = (), init_kwargs: dict | None = None
):
    """
    Get the correct handler type based on the arguments passed
    """
    if inspect.isfunction(reducer_handler):
        if len(init_args) > 0 or len(init_kwargs) > 0:
            # if the init_args or init_kwargs are passed, then the reducer_instance
            # can only be of class Reducer type
            raise TypeError("Cannot pass function handler with init args or kwargs")
        # return the function handler
        return reducer_handler
    elif not check_instance(reducer_handler, Reducer) and issubclass(reducer_handler, Reducer):
        # if handler is type of Class Reducer, create a new instance of
        # a ReducerBuilderClass
        return _ReduceBuilderClass(reducer_handler, init_args, init_kwargs)
    else:
        _LOGGER.error(
            _error_msg := f"Invalid Class Type {reducer_handler}: "
            f"Please make sure the class type is passed, and it is a subclass of Reducer"
        )
        raise TypeError(_error_msg)


class ReduceAsyncServer(NumaflowServer):
    """
    Class for a new Reduce Server instance.
    A new servicer instance is created and attached to the server.
    The server instance is returned.

    Args:
        reducer_instance: The reducer instance to be used for Reduce UDF
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
    from pynumaflow.reducer import Messages, Message, Datum, Metadata,
    ReduceAsyncServer, Reducer

    class ReduceCounter(Reducer):
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

    async def reduce_handler(
        keys: list[str], datums: AsyncIterable[Datum], md: Metadata
    ) -> Messages:
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
        invoke = os.getenv("INVOKE", "func_handler")
        if invoke == "class":
            # Here we are using the class instance as the reducer_instance
            # which will be used to invoke the handler function.
            # We are passing the init_args for the class instance.
            grpc_server = ReduceAsyncServer(ReduceCounter, init_args=(0,))
        else:
            # Here we are using the handler function directly as the reducer_instance.
            grpc_server = ReduceAsyncServer(reduce_handler)
        grpc_server.start()
    ```
    """

    def __init__(
        self,
        reducer_instance: ReduceCallable,
        init_args: tuple = (),
        init_kwargs: dict | None = None,
        sock_path=REDUCE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=REDUCE_SERVER_INFO_FILE_PATH,
        shutdown_callback=None,
    ):
        init_kwargs = init_kwargs or {}
        self.reducer_handler = get_handler(reducer_instance, init_args, init_kwargs)
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
        self.servicer = AsyncReduceServicer(self.reducer_handler)
        self._error: BaseException | None = None

    def start(self):
        """
        Starter function for the Async server class, need a separate caller
        so that all the async coroutines can be started from a single context
        """
        _LOGGER.info(
            "Starting Async Reduce Server",
        )
        aiorun.run(self.aexec(), use_uvloop=True, shutdown_callback=self.shutdown_callback)
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
        server = grpc.aio.server(options=self._server_options)
        server.add_insecure_port(self.sock_path)

        # The asyncio.Event must be created here (inside aexec) rather than in __init__,
        # because it must be bound to the running event loop that aiorun creates.
        # At __init__ time no event loop exists yet.
        shutdown_event = asyncio.Event()
        self.servicer.set_shutdown_event(shutdown_event)

        reduce_pb2_grpc.add_ReduceServicer_to_server(self.servicer, server)

        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Reducer]

        await server.start()
        info_server_write(server_info=serv_info, info_file=self.server_info_file)

        _LOGGER.info(
            "Async GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )

        async def _watch_for_shutdown():
            """Wait for the shutdown event and stop the server with a grace period."""
            await shutdown_event.wait()
            _LOGGER.info("Shutdown signal received, stopping server gracefully...")
            # Stop accepting new requests and wait for a maximum of
            # NUMAFLOW_GRPC_SHUTDOWN_GRACE_PERIOD_SECONDS seconds for in-flight requests to complete
            await server.stop(NUMAFLOW_GRPC_SHUTDOWN_GRACE_PERIOD_SECONDS)

        shutdown_task = asyncio.create_task(_watch_for_shutdown())
        try:
            await server.wait_for_termination()
        except asyncio.CancelledError:
            # SIGTERM received — aiorun cancels all tasks. We must stop
            # the gRPC server explicitly so its __del__ doesn't try to
            # schedule a coroutine on the already-closed event loop.
            _LOGGER.info("Received cancellation, stopping server gracefully...")
            await server.stop(NUMAFLOW_GRPC_SHUTDOWN_GRACE_PERIOD_SECONDS)

        # Propagate error so start() can exit with a non-zero code
        self._error = self.servicer._error

        shutdown_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await shutdown_task

        _LOGGER.info("Stopping event loop...")
        # We use aiorun to manage the event loop. The aiorun.run() runs
        # forever until loop.stop() is called. If we don't stop the
        # event loop explicitly here, the python process will not exit.
        # It reamins stuck for 5 minutes until liveness and readiness probe
        # fails enough times and k8s sends a SIGTERM
        asyncio.get_event_loop().stop()
        _LOGGER.info("Event loop stopped")
