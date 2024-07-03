import os

import aiorun
import grpc

from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    MAX_THREADS,
    _LOGGER,
    BATCH_MAP_SOCK_PATH,
    BATCH_MAP_SERVER_INFO_FILE_PATH,
)
from pynumaflow.batchmapper._dtypes import BatchMapCallable
from pynumaflow.batchmapper.servicer.async_servicer import AsyncBatchMapServicer
from pynumaflow.proto.batchmapper import batchmap_pb2_grpc
from pynumaflow.shared.server import NumaflowServer, start_async_server


class BatchMapAsyncServer(NumaflowServer):
    """
    Class for a new Map Stream Server instance.
    """

    def __init__(
        self,
        batch_mapper_instance: BatchMapCallable,
        sock_path=BATCH_MAP_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_info_file=BATCH_MAP_SERVER_INFO_FILE_PATH,
    ):
        """
        Create a new grpc Async Batch Map Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
            batch_mapper_instance: The batch map stream instance to be used for Batch Map UDF
            sock_path: The UNIX socket path to be used for the server
            max_message_size: The max message size in bytes the server can receive and send
            max_threads: The max number of threads to be spawned;
                            defaults to number of processors x4

        Example invocation:
           TODO(map-batch): add example here
        """
        self.batch_mapper_instance: BatchMapCallable = batch_mapper_instance
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.servicer = AsyncBatchMapServicer(handler=self.batch_mapper_instance)

    def start(self):
        """
        Starter function for the Async Map Stream server, we need a separate caller
        to the aexec so that all the async coroutines can be started from a single context
        """
        aiorun.run(self.aexec(), use_uvloop=True)

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
        batchmap_pb2_grpc.add_BatchMapServicer_to_server(
            self.servicer,
            server,
        )
        _LOGGER.info("Starting Batch Map Server")
        await start_async_server(
            server, self.sock_path, self.max_threads, self._server_options, self.server_info_file
        )
