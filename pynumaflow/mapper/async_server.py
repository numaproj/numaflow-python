import os

import aiorun
import grpc

from pynumaflow._constants import (
    MAX_THREADS,
    MAX_MESSAGE_SIZE,
    MAP_SOCK_PATH,
    MAP_SERVER_INFO_FILE_PATH,
)
from pynumaflow.mapper._dtypes import MapAsyncCallable
from pynumaflow.mapper.servicer.async_servicer import AsyncMapServicer
from pynumaflow.proto.mapper import map_pb2_grpc
from pynumaflow.shared.server import (
    NumaflowServer,
    start_async_server,
)


class MapAsyncServer(NumaflowServer):
    """
    Create a new grpc Map Server instance.
    Args:
        mapper_instance: The mapper instance to be used for Map UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to number of processors x4

    Example invocation:
        from pynumaflow.mapper import Messages, Message, Datum, MapAsyncServer
        async def async_map_handler(keys: list[str], datum: Datum) -> Messages:
            val = datum.value
            msg = "payload:{} event_time:{} watermark:{}".format(
                val.decode("utf-8"),
                datum.event_time,
                datum.watermark,
            )
            val = bytes(msg, encoding="utf-8")
            return Messages(Message(value=val, keys=keys))

        if __name__ == "__main__":
            grpc_server = MapAsyncServer(async_map_handler)
            grpc_server.start()
    """

    def __init__(
        self,
        mapper_instance: MapAsyncCallable,
        sock_path=MAP_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_info_file=MAP_SERVER_INFO_FILE_PATH,
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
                     defaults to number of processors x4
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self.mapper_instance = mapper_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]
        # Get the servicer instance for the async server
        self.servicer = AsyncMapServicer(handler=mapper_instance)

    def start(self) -> None:
        """
        Starter function for the Async server class, need a separate caller
        so that all the async coroutines can be started from a single context
        """
        aiorun.run(self.aexec(), use_uvloop=True)

    async def aexec(self) -> None:
        """
        Starts the Async gRPC server on the given UNIX socket with
        given max threads.
        """

        # As the server is async, we need to create a new server instance in the
        # same thread as the event loop so that all the async calls are made in the
        # same context

        server_new = grpc.aio.server()
        server_new.add_insecure_port(self.sock_path)
        map_pb2_grpc.add_MapServicer_to_server(self.servicer, server_new)

        # Start the async server
        await start_async_server(
            server_new,
            self.sock_path,
            self.max_threads,
            self._server_options,
            self.server_info_file,
        )
