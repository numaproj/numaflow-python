import os

import aiorun
import grpc
from pynumaflow.sourcer.servicer.async_servicer import AsyncSourceServicer

from pynumaflow._constants import (
    SOURCE_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    MAX_THREADS,
)
from pynumaflow.proto.sourcer import source_pb2_grpc

from pynumaflow.shared.server import NumaflowServer, start_async_server
from pynumaflow.sourcer._dtypes import SourceCallable


class SourceAsyncServer(NumaflowServer):
    """
    Class for a new Async Source Server instance.
    """

    def __init__(
        self,
        sourcer_instance: SourceCallable,
        sock_path=SOURCE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        """
        Create a new grpc Source Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
        sourcer_instance: The sourcer instance to be used for Source UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to number of processors x4
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size

        self.sourcer_instance = sourcer_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.servicer = AsyncSourceServicer(source_handler=sourcer_instance)

    def start(self):
        """
        Starter function for the Async server class, need a separate caller
        so that all the async coroutines can be started from a single context
        """
        aiorun.run(self.aexec(), use_uvloop=True)

    async def aexec(self):
        """
        Starts the Async gRPC server on the given UNIX socket with given max threads
        """

        # As the server is async, we need to create a new server instance in the
        # same thread as the event loop so that all the async calls are made in the
        # same context
        # Create a new async server instance and add the servicer to it
        server = grpc.aio.server()
        server.add_insecure_port(self.sock_path)
        source_servicer = self.servicer
        source_pb2_grpc.add_SourceServicer_to_server(source_servicer, server)
        await start_async_server(server, self.sock_path, self.max_threads, self._server_options)
