import asyncio
import contextlib
import sys

import aiorun
import grpc

from pynumaflow._constants import (
    NUM_THREADS_DEFAULT,
    MAX_MESSAGE_SIZE,
    MAX_NUM_THREADS,
    SOURCE_TRANSFORMER_SOCK_PATH,
    SOURCE_TRANSFORMER_SERVER_INFO_FILE_PATH,
    _LOGGER,
    NUMAFLOW_GRPC_SHUTDOWN_GRACE_PERIOD_SECONDS,
)
from pynumaflow.info.server import write as info_server_write
from pynumaflow.info.types import (
    ServerInfo,
    MINIMUM_NUMAFLOW_VERSION,
    ContainerType,
)
from pynumaflow.proto.sourcetransformer import transform_pb2_grpc
from pynumaflow.shared.server import NumaflowServer
from pynumaflow.sourcetransformer._dtypes import SourceTransformAsyncCallable
from pynumaflow.sourcetransformer.servicer._async_servicer import SourceTransformAsyncServicer


class SourceTransformAsyncServer(NumaflowServer):
    """
    Create a new grpc Source Transformer Server instance.
    A new servicer instance is created and attached to the server.
    The server instance is returned.

    Args:
        source_transform_instance: The source transformer instance to be used for
        Source Transformer UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to 4 and max capped at 16
        server_info_file: The path to the server info file
        shutdown_callback: Callable, executed after loop is stopped, before
                            cancelling any tasks.
                            Useful for graceful shutdown.


    Below is a simple User Defined Function example which receives a message, applies the
    following data transformation, and returns the message.

    - If the message event time is before year 2022, drop the message with event time unchanged.
    - If it's within year 2022, update the tag to `within_year_2022` and update the message
      event time to Jan 1st 2022.
    - Otherwise, (exclusively after year 2022), update the tag to `after_year_2022` and update
      the message event time to Jan 1st 2023.

    ```py
    import datetime
    import logging
    from pynumaflow.sourcetransformer import Messages, Message, Datum, SourceTransformServer

    january_first_2022 = datetime.datetime.fromtimestamp(1640995200)
    january_first_2023 = datetime.datetime.fromtimestamp(1672531200)


    async def my_handler(keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        event_time = datum.event_time
        messages = Messages()

        if event_time < january_first_2022:
            logging.info("Got event time:%s, it is before 2022, so dropping", event_time)
            messages.append(Message.to_drop(event_time))
        elif event_time < january_first_2023:
            logging.info(
                "Got event time:%s, it is within year 2022, so forwarding to within_year_2022",
                event_time,
            )
            messages.append(
                Message(value=val, event_time=january_first_2022,
                        tags=["within_year_2022"])
            )
        else:
            logging.info(
                "Got event time:%s, it is after year 2022, so forwarding to
                after_year_2022", event_time
            )
            messages.append(Message(value=val, event_time=january_first_2023,
                            tags=["after_year_2022"]))

        return messages


    if __name__ == "__main__":
        grpc_server = SourceTransformAsyncServer(my_handler)
        grpc_server.start()
    ```
    """

    def __init__(
        self,
        source_transform_instance: SourceTransformAsyncCallable,
        sock_path=SOURCE_TRANSFORMER_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=SOURCE_TRANSFORMER_SERVER_INFO_FILE_PATH,
        shutdown_callback=None,
    ):
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file
        self.shutdown_callback = shutdown_callback

        self.source_transform_instance = source_transform_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]
        self.servicer = SourceTransformAsyncServicer(handler=source_transform_instance)
        self._error: BaseException | None = None

    def start(self) -> None:
        """
        Starter function for the Async server class, need a separate caller
        so that all the async coroutines can be started from a single context
        """
        aiorun.run(self.aexec(), use_uvloop=True, shutdown_callback=self.shutdown_callback)
        if self._error:
            _LOGGER.critical("Server exiting due to UDF error: %s", self._error)
            sys.exit(1)

    async def aexec(self) -> None:
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

        transform_pb2_grpc.add_SourceTransformServicer_to_server(self.servicer, server)

        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[
            ContainerType.Sourcetransformer
        ]

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
