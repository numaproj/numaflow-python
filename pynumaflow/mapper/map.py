import asyncio
from asyncio import events

import aiorun
import grpc

from pynumaflow.info.types import Protocol
from pynumaflow._constants import MAX_THREADS, MAX_MESSAGE_SIZE, _LOGGER, MAP_SOCK_PATH, ServerType
from pynumaflow.mapper import Mapper, AsyncMapper
from pynumaflow.mapper._dtypes import MapCallable
from pynumaflow.mapper.proto import map_pb2_grpc
from pynumaflow.shared.server import prepare_server, write_info_file, NumaflowServer

_loop = None


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
        self.max_threads = max_threads
        self.max_message_size = max_message_size
        self.mapper_instance = mapper_instance
        self.server_type = server_type
        self.background_tasks = set()
        self.cleanup_coroutines = []
        self.server = self.get_server(server_type=server_type, mapper_instance=mapper_instance)

    def start(self) -> None:
        """
        Starts the gRPC server on the given UNIX socket with given max threads.
        """
        if self.server_type == ServerType.Sync:
            self.exec()
        elif self.server_type == ServerType.Async:
            try:
                loop = events.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
            # global _loop
            # _loop = loop
            aiorun.run(self.aexec())
        else:
            raise NotImplementedError

    def exec(self):
        """
        Starts the gRPC server on the given UNIX socket with given max threads.s
        """
        self.server.start()
        write_info_file(Protocol.UDS)
        _LOGGER.info(
            "Sync GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )
        self.server.wait_for_termination()

    async def aexec(self) -> None:
        """
        Starts the gRPC server on the given UNIX socket with given max threads.s
        """
        server = grpc.aio.server()
        server.add_insecure_port(self.sock_path)
        map_servicer = AsyncMapper(handler=self.mapper_instance)
        map_pb2_grpc.add_MapServicer_to_server(map_servicer, server)

        # aiorun.run(self.server.start())
        # global _loop
        # asyncio.run_coroutine_threadsafe(self.server.start(), _loop)
        # response_task = asyncio.create_task(
        #     self.server.start(),
        # )
        #
        # # Save a reference to the result of this function, to avoid a
        # # task disappearing mid-execution.
        # self.background_tasks.add(response_task)
        # response_task.add_done_callback(lambda t: self.background_tasks.remove(t))
        #
        # await response_task
        await self.server.start()

        write_info_file(Protocol.UDS)
        _LOGGER.info(
            "Async Map New GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )

        async def server_graceful_shutdown():
            """
            Shuts down the server with 5 seconds of grace period. During the
            grace period, the server won't accept new connections and allow
            existing RPCs to continue within the grace period.
            """
            _LOGGER.info("Starting graceful shutdown...")
            await self.server.stop(5)

        self.cleanup_coroutines.append(server_graceful_shutdown())
        # asyncio.run_coroutine_threadsafe(self.server.wait_for_termination(), _loop)
        await self.server.wait_for_termination()

    def get_server(self, server_type, mapper_instance: MapCallable):
        if server_type == ServerType.Sync:
            map_servicer = Mapper(handler=mapper_instance)
        elif server_type == ServerType.Async:
            map_servicer = AsyncMapper(handler=mapper_instance)
        else:
            raise NotImplementedError

        server = prepare_server(
            sock_path=self.sock_path,
            max_threads=self.max_threads,
            max_message_size=self.max_message_size,
            server_type=server_type,
        )
        map_pb2_grpc.add_MapServicer_to_server(map_servicer, server)
        return server
