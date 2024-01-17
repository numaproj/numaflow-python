import os

from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    SOURCE_TRANSFORMER_SOCK_PATH,
    MAX_THREADS,
    _LOGGER,
    UDFType,
)
from pynumaflow.shared import NumaflowServer
from pynumaflow.shared.server import sync_server_start
from pynumaflow.sourcetransformer._dtypes import SourceTransformCallable
from pynumaflow.sourcetransformer.servicer.server import SourceTransformServicer


class SourceTransformServer(NumaflowServer):
    """
    Class for a new Source Transformer Server instance.
    """

    def __init__(
        self,
        source_transform_instance: SourceTransformCallable,
        sock_path=SOURCE_TRANSFORMER_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        """
        Create a new grpc Source Transformer Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
        source_transform_instance: The source transformer instance to be used for
        Source Transformer UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to number of processors x4
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size

        self.source_transform_instance = source_transform_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]
        self.servicer = SourceTransformServicer(handler=source_transform_instance)

    def start(self):
        """
        Starts the Synchronous gRPC server on the given UNIX socket with given max threads.
        """
        _LOGGER.info(
            "Sync GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )
        # Start the sync server
        sync_server_start(
            servicer=self.servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_options=self._server_options,
            udf_type=UDFType.SourceTransformer,
        )
