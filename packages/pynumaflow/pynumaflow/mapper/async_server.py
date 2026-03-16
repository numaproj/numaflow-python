import asyncio
import contextlib
import sys

import aiorun
import grpc

from pynumaflow._constants import (
    NUM_THREADS_DEFAULT,
    MAX_MESSAGE_SIZE,
    MAP_SOCK_PATH,
    MAP_SERVER_INFO_FILE_PATH,
    MAX_NUM_THREADS,
    _LOGGER,
    NUMAFLOW_GRPC_SHUTDOWN_GRACE_PERIOD_SECONDS,
)
from pynumaflow.info.server import write as info_server_write
from pynumaflow.info.types import (
    ServerInfo,
    MAP_MODE_KEY,
    MapMode,
    MINIMUM_NUMAFLOW_VERSION,
    ContainerType,
)
from pynumaflow.mapper._dtypes import MapAsyncCallable
from pynumaflow.mapper._servicer._async_servicer import AsyncMapServicer
from pynumaflow.proto.mapper import map_pb2_grpc
from pynumaflow.shared.server import NumaflowServer


class MapAsyncServer(NumaflowServer):
    """
    Create a new grpc Map Server instance.
    Args:
        mapper_instance: The mapper instance to be used for Map UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to 4 and max capped at 16

    Example invocation:
    ```py
    from pynumaflow.mapper import Messages, Message, Datum, MapAsyncServer

    async def async_map_handler(keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        msg = (
            f"payload:{val.decode('utf-8')} "
            f"event_time:{datum.event_time} "
            f"watermark:{datum.watermark}"
        )
        return Messages(Message(value=msg.encode('utf-8'), keys=keys))

    if __name__ == "__main__":
        grpc_server = MapAsyncServer(async_map_handler)
        grpc_server.start()
    ```
    """

    def __init__(
        self,
        mapper_instance: MapAsyncCallable,
        sock_path=MAP_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=MAP_SERVER_INFO_FILE_PATH,
        shutdown_callback=None,
    ):
        """
        Create a new grpc Asynchronous Map Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.

        Args:
            mapper_instance: The mapper instance to be used for Map UDF
            sock_path: The UNIX socket path to be used for the server
            max_message_size: The max message size in bytes the server can receive and send
            max_threads: The max number of threads to be spawned;
                        defaults to 4 and max capped at 16
            server_info_file: The path to the server info file
            shutdown_callback: Callable, executed after loop is stopped, before
                            cancelling any tasks.
                            Useful for graceful shutdown.
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file
        self.shutdown_callback = shutdown_callback

        self.mapper_instance = mapper_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]
        # Get the servicer instance for the async server
        self.servicer = AsyncMapServicer(handler=mapper_instance)
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

        map_pb2_grpc.add_MapServicer_to_server(self.servicer, server)

        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Mapper]
        # Add the MAP_MODE metadata to the server info for the correct map mode
        serv_info.metadata[MAP_MODE_KEY] = MapMode.UnaryMap

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
        await server.wait_for_termination()

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
