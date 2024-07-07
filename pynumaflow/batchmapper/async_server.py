import os

import aiorun
import grpc

from pynumaflow._constants import (
    MAX_THREADS,
    MAX_MESSAGE_SIZE,
    MAP_BATCH_SOCK_PATH,
    MAP_BATCH_SERVER_INFO_FILE_PATH,
)
from pynumaflow.batchmapper._dtypes import MapBatchAsyncCallable, MapBatchAsyncUnaryCallable
from pynumaflow.batchmapper.servicer.async_servicer import (
    BatchMapServicer,
    BatchMapUnaryServicer,
    BatchMapGroupingServicer,
)
from pynumaflow.proto.batchmapper import batchmap_pb2_grpc
from pynumaflow.shared.server import (
    NumaflowServer,
    start_async_server,
)


class BatchMapAsyncServerBase(NumaflowServer):
    """
    Create a new grpc Map Server instance.
    """

    def __init__(
        self,
        servicer: BatchMapServicer,
        sock_path=MAP_BATCH_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_info_file=MAP_BATCH_SERVER_INFO_FILE_PATH,
    ):
        """
        Create a new grpc Asynchronous Map Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
        servicer: Instantiated servicer to handle messages
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        print(f"{self.sock_path}")
        print(f"{self.server_info_file}")

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.servicer = servicer

    def start(self) -> None:
        """
        Starter function for the Async server class, need a separate caller
        so that all the async coroutines can be started from a single context
        """
        print("start")
        aiorun.run(self.aexec(), use_uvloop=True)

    async def aexec(self) -> None:
        """
        Starts the Async gRPC server on the given UNIX socket with
        given max threads.
        """

        # As the server is async, we need to create a new server instance in the
        # same thread as the event loop so that all the async calls are made in the
        # same context
        server_new = grpc.aio.server()
        server_new.add_insecure_port(self.sock_path)
        batchmap_pb2_grpc.add_BatchMapServicer_to_server(self.servicer, server_new)

        # Start the async server
        await start_async_server(
            server_new,
            self.sock_path,
            self.max_threads,
            self._server_options,
            self.server_info_file,
        )


class BatchMapServer(BatchMapAsyncServerBase):
    """
    This creates a Server instance to handler an input stream of data and
    allow data to be streamed back out. The input is not bounded at the SDK level
    and handler function must be cognizant to handle to handle each message and to decide
    access patterns.

    Example invocation:
        from pynumaflow.batchmapper import (
            Message,
            Messages,
            Datum,
            BatchMapper,
            BatchMapServer,
            BatchResponses,
        )

        class MapperStreamer(BatchMapper):
            async def handler(self, datum: AsyncIterable[Datum]) -> AsyncIterable[BatchResponses]:
                async for msg in datum:
                    msgs = Messages(
                        Message(value=msv.value, keys=[], tags=[])
                    )
                    response = BatchResponses(msg.id, msgs)
                    yield response

        if __name__ == "__main__":
            handler = MapperStreamer()
            grpc_server = BatchMapServer(handler)
            grpc_server.start()
    """

    def __init__(
        self,
        mapper_instance: MapBatchAsyncCallable,
        sock_path=MAP_BATCH_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_info_file=MAP_BATCH_SERVER_INFO_FILE_PATH,
    ):
        servicer = BatchMapServicer(mapper_instance)
        super().__init__(
            servicer,
            sock_path=sock_path,
            max_message_size=max_message_size,
            max_threads=max_threads,
            server_info_file=server_info_file,
        )


class BatchMapUnaryServer(BatchMapAsyncServerBase):
    """
    This creates a server instance to handle a stream of input data and provide it to callers
    with identical interface to async_mapper's unary interface and functions as a drop-in
    replacement. Using this may provide a performance boost by utilizing the a GRPC stream
    rather than true GRPC unary communication to the numa client.
    """

    def __init__(
        self,
        mapper_instance: MapBatchAsyncUnaryCallable,
        sock_path=MAP_BATCH_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_info_file=MAP_BATCH_SERVER_INFO_FILE_PATH,
    ):
        servicer = BatchMapUnaryServicer(mapper_instance)
        super().__init__(
            servicer,
            sock_path=sock_path,
            max_message_size=max_message_size,
            max_threads=max_threads,
            server_info_file=server_info_file,
        )


class BatchMapGroupingServer(BatchMapAsyncServerBase):
    """
    This creates a server instance to force presenting messages either with a maximum number
    or after a maximum amount of time receiving. Example use cases for this are:
    * Provide max messages to control memory or resource utilization independent of readBatchSize
    * Facilitate temporal data presentation. Useful particularly if input data is not sent in a
        relatively continuous rate so waiting for max_batch_size to be reached may introduce
        too much delay
    * Future enhancements to numaflow expect to provide constant streaming of data from ISB vs
        discrete readBatchSize chunks. This will guarantee data being provided to handler.

    Additional Args:
      max_batch_size: Maximum number of messages to present to handler.
      timeout_sec: Number of seconds to wait for gathering messages. This time counting will

    Whichever field is reached first will trigger handler to be called with collected data

    Performance note: The handler interface uses AsyncInterable as input to provide identical
    drop-in support with BatchMapServer. However, the data is already present in memory by the
    time the handler is called is called, so async-for to access data will not provide any aio
    switching. Data can be immediately grouped with `[x for async for x in datums]` as desired.
    """

    def __init__(
        self,
        mapper_instance: MapBatchAsyncCallable,
        max_batch_size: int = 10,
        timeout_sec: int = 10,
        sock_path=MAP_BATCH_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_info_file=MAP_BATCH_SERVER_INFO_FILE_PATH,
    ):
        servicer = BatchMapGroupingServicer(
            mapper_instance, max_batch_size=max_batch_size, timeout_sec=timeout_sec
        )
        super().__init__(
            servicer,
            sock_path=sock_path,
            max_message_size=max_message_size,
            max_threads=max_threads,
            server_info_file=server_info_file,
        )
