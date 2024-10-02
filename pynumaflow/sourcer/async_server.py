import aiorun
import grpc

from pynumaflow.info.types import ServerInfo, ContainerType, MINIMUM_NUMAFLOW_VERSION
from pynumaflow.sourcer.servicer.async_servicer import AsyncSourceServicer

from pynumaflow._constants import (
    SOURCE_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    NUM_THREADS_DEFAULT,
    SOURCE_SERVER_INFO_FILE_PATH,
    MAX_NUM_THREADS,
)
from pynumaflow.proto.sourcer import source_pb2_grpc

from pynumaflow.shared.server import NumaflowServer, start_async_server
from pynumaflow.sourcer._dtypes import SourceCallable


class SourceAsyncServer(NumaflowServer):
    """
    Class for a new Async Source Server instance.
    """

    def __init__(
        self,
        sourcer_instance: SourceCallable,
        sock_path=SOURCE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=SOURCE_SERVER_INFO_FILE_PATH,
    ):
        """
        Create a new grpc Async Source Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
            sourcer_instance: The sourcer instance to be used for Source UDF
            sock_path: The UNIX socket path to be used for the server
            max_message_size: The max message size in bytes the server can receive and send
            max_threads: The max number of threads to be spawned;
                            defaults to 4 and max capped at 16

        Example invocation:
            from collections.abc import AsyncIterable
            from datetime import datetime
            from pynumaflow.sourcer import (
                ReadRequest,
                Message,
                AckRequest,
                PendingResponse,
                Offset,
                PartitionsResponse,
                get_default_partitions,
                Sourcer,
                SourceAsyncServer,
            )

            class AsyncSource(Sourcer):
                # AsyncSource is a class for User Defined Source implementation.

                def __init__(self):
                    # to_ack_set: Set to maintain a track of the offsets yet to be acknowledged
                    # read_idx : the offset idx till where the messages have been read
                    self.to_ack_set = set()
                    self.read_idx = 0

                async def read_handler(self, datum: ReadRequest) -> AsyncIterable[Message]:
                    # read_handler is used to read the data from the source and send
                    # the data forward
                    # for each read request we process num_records and increment
                    # the read_idx to indicate that
                    # the message has been read and the same is added to the ack set
                    if self.to_ack_set:
                        return

                    for x in range(datum.num_records):
                        yield Message(
                            payload=str(self.read_idx).encode(),
                            offset=Offset.offset_with_default_partition_id(str(self.read_idx).encode()),
                            event_time=datetime.now(),
                        )
                        self.to_ack_set.add(str(self.read_idx))
                        self.read_idx += 1

                async def ack_handler(self, ack_request: AckRequest):
                    # The ack handler is used acknowledge the offsets that have been read,
                    # and remove them from the to_ack_set
                    for offset in ack_request.offset:
                        self.to_ack_set.remove(str(offset.offset, "utf-8"))

                async def pending_handler(self) -> PendingResponse:
                    # The simple source always returns zero to indicate there is no pending record.
                    return PendingResponse(count=0)

                async def partitions_handler(self) -> PartitionsResponse:
                    # The simple source always returns default partitions.
                    return PartitionsResponse(partitions=get_default_partitions())

            if __name__ == "__main__":
                ud_source = AsyncSource()
                grpc_server = SourceAsyncServer(ud_source)
                grpc_server.start()

        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self.sourcer_instance = sourcer_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.servicer = AsyncSourceServicer(source_handler=sourcer_instance)

    def start(self):
        """
        Starter function for the Async server class, need a separate caller
        so that all the async coroutines can be started from a single context
        """
        aiorun.run(self.aexec(), use_uvloop=True)

    async def aexec(self):
        """
        Starts the Async gRPC server on the given UNIX socket with given max threads
        """
        # As the server is async, we need to create a new server instance in the
        # same thread as the event loop so that all the async calls are made in the
        # same context
        # Create a new async server instance and add the servicer to it
        server = grpc.aio.server(options=self._server_options)
        server.add_insecure_port(self.sock_path)
        source_servicer = self.servicer
        source_pb2_grpc.add_SourceServicer_to_server(source_servicer, server)

        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Sourcer]
        # Start the async server
        await start_async_server(
            server_async=server,
            sock_path=self.sock_path,
            max_threads=self.max_threads,
            cleanup_coroutines=list(),
            server_info_file=self.server_info_file,
            server_info=serv_info,
        )
