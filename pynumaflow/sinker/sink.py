import os

import aiorun
import grpc

from pynumaflow.sinker.async_sink import AsyncSinker
from pynumaflow.sinker.proto import sink_pb2_grpc

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
    def __init__(
        self,
        sinker_instance: SinkCallable,
        sock_path=SINK_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_type=ServerType.Sync,
    ):
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
        sink_servicer = self.get_servicer(
            sinker_instance=self.sinker_instance, server_type=self.server_type
        )
        _LOGGER.info(
            "Sync GRPC Sink listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )

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
        server = grpc.aio.server()
        server.add_insecure_port(self.sock_path)
        sink_servicer = self.get_servicer(
            sinker_instance=self.sinker_instance, server_type=self.server_type
        )
        sink_pb2_grpc.add_SinkServicer_to_server(sink_servicer, server)
        await start_async_server(server, self.sock_path, self.max_threads, self._server_options)

    def get_servicer(self, sinker_instance: SinkCallable, server_type: ServerType):
        if server_type == ServerType.Sync:
            return Sinker(sinker_instance)
        elif server_type == ServerType.Async:
            return AsyncSinker(sinker_instance)
        else:
            raise NotImplementedError
