import os

import aiorun
import grpc
from pynumaflow.sourcer.async_server import AsyncSourcer
from pynumaflow.sourcer.server import Sourcer

from pynumaflow._constants import (
    SOURCE_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    MAX_THREADS,
    ServerType,
    _LOGGER,
    UDFType,
)
from pynumaflow.proto.sourcer import source_pb2_grpc

from pynumaflow.shared.server import NumaflowServer, sync_server_start, start_async_server
from pynumaflow.sourcer._dtypes import SourceCallable


class SourceServer(NumaflowServer):
    def __init__(
        self,
        sourcer_instance: SourceCallable,
        sock_path=SOURCE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_type=ServerType.Sync,
    ):
        """
        Create a new grpc Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.

        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size

        self.sourcer_instance = sourcer_instance
        self.server_type = server_type

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

    def start(self):
        """
        Starts the gRPC server on the given UNIX socket with given max threads.
        """
        if self.server_type == ServerType.Sync:
            self.exec()
        elif self.server_type == ServerType.Async:
            aiorun.run(self.aexec())
        else:
            _LOGGER.error("Server type not supported", self.server_type)
            raise NotImplementedError

    def exec(self):
        """
        Starts the Synchronous gRPC server on the given UNIX socket with given max threads.
        """
        source_servicer = self.get_servicer(
            sourcer_instance=self.sourcer_instance, server_type=self.server_type
        )
        _LOGGER.info(
            "Sync GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )

        sync_server_start(
            servicer=source_servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_options=self._server_options,
            udf_type=UDFType.Source,
        )

    async def aexec(self):
        """
        Starts the Async gRPC server on the given UNIX socket with given max threads
        """
        server = grpc.aio.server()
        server.add_insecure_port(self.sock_path)
        source_servicer = self.get_servicer(
            sourcer_instance=self.sourcer_instance, server_type=self.server_type
        )
        source_pb2_grpc.add_SourceServicer_to_server(source_servicer, server)
        await start_async_server(server, self.sock_path, self.max_threads, self._server_options)

    def get_servicer(self, sourcer_instance: SourceCallable, server_type: ServerType):
        if server_type == ServerType.Sync:
            source_servicer = Sourcer(source_handler=sourcer_instance)
        elif server_type == ServerType.Async:
            source_servicer = AsyncSourcer(source_handler=sourcer_instance)
        else:
            raise NotImplementedError
        return source_servicer
