import os

import aiorun
import grpc
from pynumaflow.mapper.async_server import AsyncMapper

from pynumaflow.mapper.server import Mapper

from pynumaflow._constants import (
    MAX_THREADS,
    MAX_MESSAGE_SIZE,
    _LOGGER,
    MAP_SOCK_PATH,
    ServerType,
    UDFType,
)

from pynumaflow.mapper._dtypes import MapCallable
from pynumaflow.proto.mapper import map_pb2_grpc
from pynumaflow.shared.server import (
    NumaflowServer,
    start_async_server,
    start_multiproc_server,
    sync_server_start,
)


class MapServer(NumaflowServer):
    """
    Create a new grpc Map Server instance.
    """

    def __init__(
        self,
        mapper_instance: MapCallable,
        sock_path=MAP_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_type=ServerType.Sync,
    ):
        """
        Create a new grpc Map Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
        mapper_instance: The mapper instance to be used for Map UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4
        server_type: The type of server to be used, this can be one of the following:
                        - ServerType.Sync: Synchronous server
                        - ServerType.Async: Asynchronous server
                        - ServerType.Multiproc: Multiprocess server
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size

        self.mapper_instance = mapper_instance
        self.server_type = server_type

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]
        if server_type == ServerType.Multiproc:
            self._server_options.append(("grpc.so_reuseport", 1))
            self._server_options.append(("grpc.so_reuseaddr", 1))

        # Set the number of processes to be spawned to the number of CPUs or
        # the value of the env var NUM_CPU_MULTIPROC defined by the user
        # Setting the max value to 2 * CPU count
        # Used for multiproc server
        self._process_count = min(
            int(os.getenv("NUM_CPU_MULTIPROC", str(os.cpu_count()))), 2 * os.cpu_count()
        )

    def start(self) -> None:
        """
        Starter function for the server class, Handles the server type and
        starts the server accordingly. If the server type is not supported,
        raises NotImplementedError.
        Currently supported server types are:
            - ServerType.Sync: Synchronous server
            - ServerType.Async: Asynchronous server
            - ServerType.Multiproc: Multiprocess server
        """
        if self.server_type == ServerType.Sync:
            self.exec()
        elif self.server_type == ServerType.Async:
            aiorun.run(self.aexec())
        elif self.server_type == ServerType.Multiproc:
            self.exec_multiproc()
        else:
            _LOGGER.error("Server type not supported - %s", str(self.server_type))
            raise NotImplementedError

    def exec(self):
        """
        Starts the Synchronous gRPC server on the given UNIX socket with given max threads.
        """
        # Get the servicer instance based on the server type
        map_servicer = self.get_servicer(
            mapper_instance=self.mapper_instance, server_type=self.server_type
        )
        _LOGGER.info(
            "Sync GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )
        # Start the server
        sync_server_start(
            servicer=map_servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_options=self._server_options,
            udf_type=UDFType.Map,
        )

    def exec_multiproc(self):
        """
        Starts the multirpoc gRPC server on the given UNIX socket with
        given max threads.
        """

        # Get the servicer instance based on the server type
        map_servicer = self.get_servicer(
            mapper_instance=self.mapper_instance, server_type=self.server_type
        )

        # Start the multirpoc server
        start_multiproc_server(
            max_threads=self.max_threads,
            servicer=map_servicer,
            process_count=self._process_count,
            server_options=self._server_options,
            udf_type=UDFType.Map,
        )

    async def aexec(self) -> None:
        """
        Starts the Async gRPC server on the given UNIX socket with
        given max threads.
        """

        # As the server is async, we need to create a new server instance in the
        # same thread as the event loop so that all the async calls are made in the
        # same context
        # Create a new async server instance and add the servicer to it
        server_new = grpc.aio.server()
        server_new.add_insecure_port(self.sock_path)
        map_servicer = self.get_servicer(
            mapper_instance=self.mapper_instance, server_type=self.server_type
        )
        map_pb2_grpc.add_MapServicer_to_server(map_servicer, server_new)

        # Start the async server
        await start_async_server(server_new, self.sock_path, self.max_threads, self._server_options)

    def get_servicer(self, mapper_instance: MapCallable, server_type: ServerType):
        """Returns the servicer instance based on the server type"""
        if server_type == ServerType.Sync:
            map_servicer = Mapper(handler=mapper_instance)
        elif server_type == ServerType.Async:
            map_servicer = AsyncMapper(handler=mapper_instance)
        elif server_type == ServerType.Multiproc:
            map_servicer = Mapper(handler=mapper_instance)
        return map_servicer
