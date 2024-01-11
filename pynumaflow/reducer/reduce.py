import aiorun
import grpc

from pynumaflow.proto.reducer import reduce_pb2_grpc

from pynumaflow.reducer.async_server import AsyncReducer

from pynumaflow._constants import (
    REDUCE_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    MAX_THREADS,
    ServerType,
    _LOGGER,
)

from pynumaflow.reducer._dtypes import ReduceCallable

from pynumaflow.shared.server import NumaflowServer, start_async_server


class ReduceServer(NumaflowServer):
    """
    Class for a new Reduce Server instance.
    """

    def __init__(
        self,
        reducer_instance: ReduceCallable,
        sock_path=REDUCE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_type=ServerType.Async,
    ):
        """
        Create a new grpc Reduce Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
        reducer_instance: The reducer instance to be used for Reduce UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to number of processors x4
        server_type: The type of server to be used
        """
        self.reducer_instance: ReduceCallable = reducer_instance
        self.sock_path = f"unix://{sock_path}"
        self.max_message_size = max_message_size
        self.max_threads = max_threads
        self.server_type = server_type

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

    def start(self):
        """
        Starter function for the Reduce server, Handles the server type and
        starts the server.
        Currently supported server types are:
        1. Async
        """
        if self.server_type == ServerType.Async:
            aiorun.run(self.aexec(), use_uvloop=True)
        else:
            _LOGGER.error("Server type not supported - %s", str(self.server_type))
            raise NotImplementedError

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
        reduce_servicer = self.get_servicer(
            reducer_instance=self.reducer_instance, server_type=self.server_type
        )
        reduce_pb2_grpc.add_ReduceServicer_to_server(reduce_servicer, server)
        await start_async_server(server, self.sock_path, self.max_threads, self._server_options)

    def get_servicer(self, reducer_instance: ReduceCallable, server_type: ServerType):
        """Get the servicer instance for the given server type"""
        if server_type == ServerType.Async:
            return AsyncReducer(reducer_instance)
