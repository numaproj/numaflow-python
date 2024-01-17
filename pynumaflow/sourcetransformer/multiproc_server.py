import os

from pynumaflow.sourcetransformer.servicer.server import SourceTransformServicer

from pynumaflow.shared.server import start_multiproc_server

from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    SOURCE_TRANSFORMER_SOCK_PATH,
    MAX_THREADS,
    UDFType,
    _PROCESS_COUNT,
)

from pynumaflow.sourcetransformer._dtypes import SourceTransformCallable

from pynumaflow.shared import NumaflowServer


class SourceTransformMultiProcServer(NumaflowServer):
    """
    Class for a new Source Transformer Server instance.
    """

    def __init__(
        self,
        source_transform_instance: SourceTransformCallable,
        server_count: int = _PROCESS_COUNT,
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
            ("grpc.so_reuseport", 1),
            ("grpc.so_reuseaddr", 1),
        ]
        # Set the number of processes to be spawned to the number of CPUs or
        # the value of the env var NUM_CPU_MULTIPROC defined by the user
        # Setting the max value to 2 * CPU count
        # Used for multiproc server
        self._process_count = min(server_count, 2 * _PROCESS_COUNT)
        self.servicer = SourceTransformServicer(handler=source_transform_instance)

    def start(self):
        """
        Starts the Multiproc gRPC server on the given TCP sockets
        with given max threads.
        """
        start_multiproc_server(
            max_threads=self.max_threads,
            servicer=self.servicer,
            process_count=self._process_count,
            server_options=self._server_options,
            udf_type=UDFType.Map,
        )
