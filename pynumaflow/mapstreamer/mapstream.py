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
        """ """
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
        if self.server_type == ServerType.Async:
            aiorun.run(self.aexec())
        else:
            _LOGGER.error("Server type not supported", self.server_type)
            raise NotImplementedError

    async def aexec(self):
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
        if server_type == ServerType.Async:
            map_servicer = AsyncMapStreamer(handler=map_stream_instance)
        else:
            raise NotImplementedError
        return map_servicer
