import os

from pynumaflow._constants import (
    SOURCE_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    MAX_THREADS,
    _LOGGER,
    UDFType,
    SOURCE_SERVER_INFO_FILE_PATH,
)
from pynumaflow.shared.server import NumaflowServer, sync_server_start
from pynumaflow.sourcer._dtypes import SourceCallable
from pynumaflow.sourcer.servicer.sync_servicer import SyncSourceServicer


class SourceServer(NumaflowServer):
    """
    Class for a new Source Server instance.
    """

    def __init__(
        self,
        sourcer_instance: SourceCallable,
        sock_path=SOURCE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_info_file=SOURCE_SERVER_INFO_FILE_PATH,
    ):
        """
        Create a new grpc Source Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
            sourcer_instance: The sourcer instance to be used for Source UDF
            sock_path: The UNIX socket path to be used for the server
            max_message_size: The max message size in bytes the server can receive and send
            max_threads: The max number of threads to be spawned;
                            defaults to number of processors x4

        Example invocation:
            from collections.abc import Iterable
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
                SourceServer,
            )

            class SimpleSource(Sourcer):
                # SimpleSource is a class for User Defined Source implementation.

                def __init__(self):
                    # to_ack_set: Set to maintain a track of the offsets yet to be acknowledged
                    # read_idx : the offset idx till where the messages have been read
                    self.to_ack_set = set()
                    self.read_idx = 0

                def read_handler(self, datum: ReadRequest) -> Iterable[Message]:
                    # read_handler is used to read the data from the source and
                    # send the data forward
                    # for each read request we process num_records and increment the
                    # read_idx to indicate that
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

                def ack_handler(self, ack_request: AckRequest):
                    # The ack handler is used acknowledge the offsets that have been
                    # read, and remove them
                    # from the to_ack_set
                    for offset in ack_request.offset:
                        self.to_ack_set.remove(str(offset.offset, "utf-8"))

                def pending_handler(self) -> PendingResponse:
                    # The simple source always returns zero to indicate there is no pending record.
                    return PendingResponse(count=0)

                def partitions_handler(self) -> PartitionsResponse:
                    # The simple source always returns zero to indicate there is no pending record.
                    return PartitionsResponse(partitions=get_default_partitions())

            if __name__ == "__main__":
                ud_source = SimpleSource()
                grpc_server = SourceServer(ud_source)
                grpc_server.start()
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self.sourcer_instance = sourcer_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.servicer = SyncSourceServicer(source_handler=sourcer_instance)

    def start(self):
        """
        Starts the Synchronous Source gRPC server on the given
        UNIX socket with given max threads.
        """
        # Get the servicer instance
        source_servicer = self.servicer
        _LOGGER.info(
            "Sync Source GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )
        # Start the sync server
        sync_server_start(
            servicer=source_servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_info_file=self.server_info_file,
            server_options=self._server_options,
            udf_type=UDFType.Source,
        )
