import os

import aiorun
import grpc

from pynumaflow.sinker.async_sink import AsyncSinker
from pynumaflow.proto.sinker import sink_pb2_grpc

from pynumaflow.sinker.server import Sinker

from pynumaflow._constants import (
    SINK_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    MAX_THREADS,
    ServerType,
    _LOGGER,
    UDFType,
)

from pynumaflow.shared.server import NumaflowServer, sync_server_start, start_async_server
from pynumaflow.sinker._dtypes import SinkCallable


class SinkServer(NumaflowServer):
    """
    SinkServer is the main class to start a gRPC server for a sinker.
    """

    def __init__(
        self,
        sinker_instance: SinkCallable,
        sock_path=SINK_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_type=ServerType.Sync,
    ):
        """
        Create a new grpc Sink Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
        sinker_instance: The sinker instance to be used for Sink UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to number of processors x4
        server_type: The type of server to be used, this can be one of the following:
                        - ServerType.Sync: Synchronous server
                        - ServerType.Async: Asynchronous server
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size

        self.sinker_instance = sinker_instance
        self.server_type = server_type

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

    def start(self):
        """
        Starter function for the server class, Handles the server type and
        starts the server accordingly. If the server type is not supported,
        raises NotImplementedError.
        Currently supported server types are:
            - ServerType.Sync: Synchronous server
            - ServerType.Async: Asynchronous server
        """
        if self.server_type == ServerType.Sync:
            self.exec()
        elif self.server_type == ServerType.Async:
            aiorun.run(self.aexec())
        else:
            _LOGGER.error("Server type not supported - %s", str(self.server_type))
            raise NotImplementedError

    def exec(self):
        """
        Starts the Synchronous gRPC server on the
        given UNIX socket with given max threads.
        """
        # Get the servicer instance
        sink_servicer = self.get_servicer(
            sinker_instance=self.sinker_instance, server_type=self.server_type
        )
        _LOGGER.info(
            "Sync GRPC Sink listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )
        # Start the server
        sync_server_start(
            servicer=sink_servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_options=self._server_options,
            udf_type=UDFType.Sink,
        )

    async def aexec(self):
        """
        Starts the Asynchronous gRPC server on the given UNIX socket with given max threads.
        """
        # As the server is async, we need to create a new server instance in the
        # same thread as the event loop so that all the async calls are made in the
        # same context
        # Create a new server instance, add the servicer to it and start the server
        server = grpc.aio.server()
        server.add_insecure_port(self.sock_path)
        sink_servicer = self.get_servicer(
            sinker_instance=self.sinker_instance, server_type=self.server_type
        )
        sink_pb2_grpc.add_SinkServicer_to_server(sink_servicer, server)
        await start_async_server(server, self.sock_path, self.max_threads, self._server_options)

    def get_servicer(self, sinker_instance: SinkCallable, server_type: ServerType):
        """
        Returns the servicer instance based on the server type.
        """
        if server_type == ServerType.Sync:
            return Sinker(sinker_instance)
        elif server_type == ServerType.Async:
            return AsyncSinker(sinker_instance)
