import os

from pynumaflow._constants import (
    SOURCE_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    MAX_THREADS,
    _LOGGER,
    UDFType,
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
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size

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
            server_options=self._server_options,
            udf_type=UDFType.Source,
        )
