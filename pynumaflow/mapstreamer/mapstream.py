import os

import aiorun
import grpc

from pynumaflow.mapstreamer.async_server import AsyncMapStreamer
from pynumaflow.proto.mapstreamer import mapstream_pb2_grpc

from pynumaflow._constants import (
    MAP_STREAM_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    MAX_THREADS,
    ServerType,
    _LOGGER,
)

from pynumaflow.mapstreamer._dtypes import MapStreamCallable

from pynumaflow.shared.server import NumaflowServer, start_async_server


class MapStreamServer(NumaflowServer):
    """
    Class for a new Map Stream Server instance.
    """

    def __init__(
        self,
        map_stream_instance: MapStreamCallable,
        sock_path=MAP_STREAM_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_type=ServerType.Async,
    ):
        """
        Create a new grpc Map Stream Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
        map_stream_instance: The map stream instance to be used for Map Stream UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to number of processors x4
        server_type: The type of server to be used
        """
        self.map_stream_instance: MapStreamCallable = map_stream_instance
        self.sock_path = f"unix://{sock_path}"
        self.max_message_size = max_message_size
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.server_type = server_type

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

    def start(self):
        """
        Starter function for the Map Stream server, Handles the server type and
        starts the server accordingly. If the server type is not supported,
        raises NotImplementedError.
        Currently supported server types are:
            - ServerType.Async: Asynchronous server
        """
        if self.server_type == ServerType.Async:
            aiorun.run(self.aexec(), use_uvloop=True)
        else:
            _LOGGER.error("Server type not supported - %s", str(self.server_type))
            raise NotImplementedError

    async def aexec(self):
        """
        Starts the Async gRPC server on the given UNIX socket with
        given max threads.
        """
        # As the server is async, we need to create a new server instance in the
        # same thread as the event loop so that all the async calls are made in the
        # same context
        # Create a new async server instance and add the servicer to it
        server = grpc.aio.server()
        server.add_insecure_port(self.sock_path)
        map_servicer = self.get_servicer(
            map_stream_instance=self.map_stream_instance, server_type=self.server_type
        )
        mapstream_pb2_grpc.add_MapStreamServicer_to_server(
            map_servicer,
            server,
        )
        _LOGGER.info("Starting Map Stream Server")
        await start_async_server(server, self.sock_path, self.max_threads, self._server_options)

    def get_servicer(self, map_stream_instance: MapStreamCallable, server_type: ServerType):
        """
        Returns the servicer instance based on the server type.
        Currently supported server types are:
            - ServerType.Async: Asynchronous server
        """
        if server_type == ServerType.Async:
            map_servicer = AsyncMapStreamer(handler=map_stream_instance)
        else:
            raise NotImplementedError
        return map_servicer
