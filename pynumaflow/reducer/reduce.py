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
    def __init__(
        self,
        reducer_instance: ReduceCallable,
        sock_path=REDUCE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_type=ServerType.Async,
    ):
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
        if self.server_type == ServerType.Async:
            aiorun.run(self.aexec())
        else:
            _LOGGER.error("Server type not supported", self.server_type)
            raise NotImplementedError

    async def aexec(self):
        server = grpc.aio.server()
        server.add_insecure_port(self.sock_path)
        reduce_servicer = self.get_servicer(
            reducer_instance=self.reducer_instance, server_type=self.server_type
        )
        reduce_pb2_grpc.add_ReduceServicer_to_server(reduce_servicer, server)
        await start_async_server(server, self.sock_path, self.max_threads, self._server_options)

    def get_servicer(self, reducer_instance: ReduceCallable, server_type: ServerType):
        if server_type == ServerType.Async:
            return AsyncReducer(reducer_instance)
        else:
            raise NotImplementedError
