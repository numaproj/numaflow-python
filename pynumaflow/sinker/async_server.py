import os

import aiorun
import grpc

from pynumaflow.info.types import ContainerType, ServerInfo, MINIMUM_NUMAFLOW_VERSION
from pynumaflow.sinker.servicer.async_servicer import AsyncSinkServicer
from pynumaflow.proto.sinker import sink_pb2_grpc


from pynumaflow._constants import (
    SINK_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    NUM_THREADS_DEFAULT,
    SINK_SERVER_INFO_FILE_PATH,
    ENV_UD_CONTAINER_TYPE,
    UD_CONTAINER_FALLBACK_SINK,
    _LOGGER,
    FALLBACK_SINK_SOCK_PATH,
    FALLBACK_SINK_SERVER_INFO_FILE_PATH,
    MAX_NUM_THREADS,
)

from pynumaflow.shared.server import NumaflowServer, start_async_server
from pynumaflow.sinker._dtypes import SinkAsyncCallable


class SinkAsyncServer(NumaflowServer):
    """
    SinkAsyncServer is the main class to start a gRPC server for a sinker.
    Create a new grpc Async Sink Server instance.
    A new servicer instance is created and attached to the server.
    The server instance is returned.
    Args:
        sinker_instance: The sinker instance to be used for Sink UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to 4 and max capped at 16

    Example invocation:
        import os
        from collections.abc import AsyncIterable
        from pynumaflow.sinker import Datum, Responses, Response, Sinker
        from pynumaflow.sinker import SinkAsyncServer
        from pynumaflow._constants import _LOGGER


        class UserDefinedSink(Sinker):
            async def handler(self, datums: AsyncIterable[Datum]) -> Responses:
                responses = Responses()
                async for msg in datums:
                    _LOGGER.info("User Defined Sink %s", msg.value.decode("utf-8"))
                    responses.append(Response.as_success(msg.id))
                return responses


        async def udsink_handler(datums: AsyncIterable[Datum]) -> Responses:
            responses = Responses()
            async for msg in datums:
                _LOGGER.info("User Defined Sink %s", msg.value.decode("utf-8"))
                responses.append(Response.as_success(msg.id))
            return responses


        if __name__ == "__main__":
            invoke = os.getenv("INVOKE", "func_handler")
            if invoke == "class":
                sink_handler = UserDefinedSink()
            else:
                sink_handler = udsink_handler
            grpc_server = SinkAsyncServer(sink_handler)
            grpc_server.start()
    """

    def __init__(
        self,
        sinker_instance: SinkAsyncCallable,
        sock_path=SINK_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=SINK_SERVER_INFO_FILE_PATH,
    ):
        # If the container type is fallback sink, then use the fallback sink address and path.
        if os.getenv(ENV_UD_CONTAINER_TYPE, "") == UD_CONTAINER_FALLBACK_SINK:
            _LOGGER.info("Using Fallback Sink")
            sock_path = FALLBACK_SINK_SOCK_PATH
            server_info_file = FALLBACK_SINK_SERVER_INFO_FILE_PATH

        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self.sinker_instance = sinker_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.servicer = AsyncSinkServicer(sinker_instance)

    def start(self):
        """
        Starter function for the Async server class, need a separate caller
        so that all the async coroutines can be started from a single context
        """
        aiorun.run(self.aexec(), use_uvloop=True)

    async def aexec(self):
        """
        Starts the Asynchronous gRPC server on the given UNIX socket with given max threads.
        """
        # As the server is async, we need to create a new server instance in the
        # same thread as the event loop so that all the async calls are made in the
        # same context
        # Create a new server instance, add the servicer to it and start the server
        server = grpc.aio.server(options=self._server_options)
        server.add_insecure_port(self.sock_path)
        sink_pb2_grpc.add_SinkServicer_to_server(self.servicer, server)
        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Sinker]
        await start_async_server(
            server_async=server,
            sock_path=self.sock_path,
            max_threads=self.max_threads,
            cleanup_coroutines=list(),
            server_info_file=self.server_info_file,
            server_info=serv_info,
        )
