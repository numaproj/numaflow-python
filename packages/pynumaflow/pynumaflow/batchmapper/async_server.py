import aiorun
import grpc

from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    NUM_THREADS_DEFAULT,
    _LOGGER,
    BATCH_MAP_SOCK_PATH,
    MAP_SERVER_INFO_FILE_PATH,
    MAX_NUM_THREADS,
)
from pynumaflow.batchmapper._dtypes import BatchMapCallable
from pynumaflow.batchmapper.servicer.async_servicer import AsyncBatchMapServicer
from pynumaflow.info.types import (
    ServerInfo,
    MAP_MODE_KEY,
    MapMode,
    MINIMUM_NUMAFLOW_VERSION,
    ContainerType,
)
from pynumaflow.proto.mapper import map_pb2_grpc
from pynumaflow.shared.server import NumaflowServer, start_async_server


class BatchMapAsyncServer(NumaflowServer):
    """
    Class for a new Batch Map Async Server instance.
    """

    def __init__(
        self,
        batch_mapper_instance: BatchMapCallable,
        sock_path=BATCH_MAP_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=MAP_SERVER_INFO_FILE_PATH,
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
                            defaults to 4 and max capped at 16

        Example invocation:
         class Flatmap(BatchMapper):
            async def handler(
                self,
                datums: AsyncIterable[Datum],
            ) -> BatchResponses:
                batch_responses = BatchResponses()
                async for datum in datums:
                    val = datum.value
                    _ = datum.event_time
                    _ = datum.watermark
                    strs = val.decode("utf-8").split(",")
                    batch_response = BatchResponse.from_id(datum.id)
                    if len(strs) == 0:
                        batch_response.append(Message.to_drop())
                    else:
                        for s in strs:
                            batch_response.append(Message(str.encode(s)))
                    batch_responses.append(batch_response)

                return batch_responses

        if __name__ == "__main__":
            grpc_server = BatchMapAsyncServer(Flatmap())
            grpc_server.start()
        """
        self.batch_mapper_instance: BatchMapCallable = batch_mapper_instance
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.servicer = AsyncBatchMapServicer(handler=self.batch_mapper_instance)

    def start(self):
        """
        Starter function for the Async Batch Map server, we need a separate caller
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
        server = grpc.aio.server(options=self._server_options)
        server.add_insecure_port(self.sock_path)
        map_pb2_grpc.add_MapServicer_to_server(
            self.servicer,
            server,
        )
        _LOGGER.info("Starting Batch Map Server")
        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Mapper]
        # Add the MAP_MODE metadata to the server info for the correct map mode
        serv_info.metadata[MAP_MODE_KEY] = MapMode.BatchMap

        # Start the async server
        await start_async_server(
            server_async=server,
            sock_path=self.sock_path,
            max_threads=self.max_threads,
            cleanup_coroutines=list(),
            server_info_file=self.server_info_file,
            server_info=serv_info,
        )
