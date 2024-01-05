import os

import aiorun
import grpc

from pynumaflow._constants import MAX_THREADS, MAX_MESSAGE_SIZE, _LOGGER, MAP_SOCK_PATH, ServerType
from pynumaflow.mapper import Mapper, AsyncMapper
from pynumaflow.mapper._dtypes import MapCallable
from pynumaflow.mapper.proto import map_pb2_grpc
from pynumaflow.shared.server import (
    prepare_server,
    NumaflowServer,
    start_async_server,
    start_sync_server,
    start_multiproc_server,
)


class MapServer(NumaflowServer):
    """
    Create a new grpc Server instance.
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

        self.mapper_instance = mapper_instance
        self.server_type = server_type
        self.background_tasks = set()
        self.cleanup_coroutines = []

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
            ("grpc.so_reuseport", 1),
            ("grpc.so_reuseaddr", 1),
        ]
        # Set the number of processes to be spawned to the number of CPUs or
        # the value of the env var NUM_CPU_MULTIPROC defined by the user
        # Setting the max value to 2 * CPU count
        # Used for multiproc server
        self._process_count = min(
            int(os.getenv("NUM_CPU_MULTIPROC", str(os.cpu_count()))), 2 * os.cpu_count()
        )

        # Get the server instance based on the server type and assign it to self.server
        # self.server = self.get_server(server_type=server_type, mapper_instance=mapper_instance)

    def start(self) -> None:
        """
        Starts the gRPC server on the given UNIX socket with given max threads.
        """
        if self.server_type == ServerType.Sync:
            self.exec()
        elif self.server_type == ServerType.Async:
            _LOGGER.info("Starting Async Map Server with aiorun...")
            aiorun.run(self.aexec())
        elif self.server_type == ServerType.Multiproc:
            self.exec_multiproc()
            raise NotImplementedError

        else:
            raise NotImplementedError

    def exec(self):
        """
        Starts the Synchronous gRPC server on the given UNIX socket with given max threads.
        """
        server = prepare_server(self.sock_path, self.max_threads, self._server_options)
        map_servicer = self.get_servicer(
            mapper_instance=self.mapper_instance, server_type=self.server_type
        )
        map_pb2_grpc.add_MapServicer_to_server(map_servicer, server)
        # server.start()
        # write_info_file(Protocol.UDS)
        # _LOGGER.info(
        #     "Sync GRPC Server listening on: %s with max threads: %s",
        #     self.sock_path,
        #     self.max_threads,
        # )
        # server.wait_for_termination()
        # Log the server start
        _LOGGER.info(
            "Sync GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )
        start_sync_server(server=server)

    def exec_multiproc(self):
        """
        Starts the gRPC server on the given UNIX socket with given max threads.
        """
        servers, server_ports = prepare_server(
            server_type=self.server_type,
            max_threads=self.max_threads,
            server_options=self._server_options,
            process_count=self._process_count,
            sock_path=self.sock_path,
        )

        map_servicer = self.get_servicer(
            mapper_instance=self.mapper_instance, server_type=self.server_type
        )
        for server in servers:
            map_pb2_grpc.add_MapServicer_to_server(map_servicer, server)

        start_multiproc_server(
            servers=servers, server_ports=server_ports, max_threads=self.max_threads
        )
        # server.start()
        # write_info_file(Protocol.UDS)
        # _LOGGER.info(
        #     "Sync GRPC Server listening on: %s with max threads: %s",
        #     self.sock_path,
        #     self.max_threads,
        # )
        # server.wait_for_termination()

    async def aexec(self) -> None:
        """
        Starts the Async gRPC server on the given UNIX socket with given max threads.s
        """
        server_new = grpc.aio.server()
        server_new.add_insecure_port(self.sock_path)
        map_servicer = self.get_servicer(
            mapper_instance=self.mapper_instance, server_type=self.server_type
        )
        map_pb2_grpc.add_MapServicer_to_server(map_servicer, server_new)

        await start_async_server(server_new, self.sock_path, self.max_threads, self._server_options)

        # await server_new.start()
        #
        # write_info_file(Protocol.UDS)
        # _LOGGER.info(
        #     "Async Map New GRPC Server listening on: %s with max threads: %s",
        #     self.sock_path,
        #     self.max_threads,
        # )
        #
        # async def server_graceful_shutdown():
        #     """
        #     Shuts down the server with 5 seconds of grace period. During the
        #     grace period, the server won't accept new connections and allow
        #     existing RPCs to continue within the grace period.
        #     """
        #     _LOGGER.info("Starting graceful shutdown...")
        #     await server_new.stop(5)
        #
        # self.cleanup_coroutines.append(server_graceful_shutdown())
        # # asyncio.run_coroutine_threadsafe(self.server.wait_for_termination(), _loop)
        # await server_new.wait_for_termination()

    def get_servicer(self, mapper_instance: MapCallable, server_type: ServerType):
        if server_type == ServerType.Sync:
            map_servicer = Mapper(handler=mapper_instance)
        elif server_type == ServerType.Async:
            map_servicer = AsyncMapper(handler=mapper_instance)
        elif server_type == ServerType.Multiproc:
            map_servicer = Mapper(handler=mapper_instance)
        else:
            raise NotImplementedError
        return map_servicer
