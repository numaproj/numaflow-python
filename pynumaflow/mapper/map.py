import aiorun

from pynumaflow.info.types import Protocol
from pynumaflow._constants import MAX_THREADS, MAX_MESSAGE_SIZE, _LOGGER, MAP_SOCK_PATH, ServerType
from pynumaflow.mapper import Mapper, AsyncMapper
from pynumaflow.mapper._dtypes import MapCallable
from pynumaflow.mapper.proto import map_pb2_grpc
from pynumaflow.shared.server import prepare_server, write_info_file, NumaflowServer


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
        self.server_type = server_type
        self.server = self.get_server(server_type=server_type, mapper_instance=mapper_instance)

    def start(self) -> None:
        """
        Starts the gRPC server on the given UNIX socket with given max threads.
        """
        if self.server_type == ServerType.Sync:
            self.exec()
        elif self.server_type == ServerType.Async:
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

    async def aexec(self):
        """
        Starts the gRPC server on the given UNIX socket with given max threads.s
        """
        aiorun.run(self.server.start())
        # await self.server.start()
        write_info_file(Protocol.UDS)
        _LOGGER.info(
            "Async GRPC Server listening on: %s with max threads: %s",
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

        self.server.cleanup_coroutines.append(server_graceful_shutdown())
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
