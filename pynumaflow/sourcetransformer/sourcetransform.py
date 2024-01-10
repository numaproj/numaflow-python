import os

from pynumaflow.sourcetransformer.server import SourceTransformer

from pynumaflow.shared.server import sync_server_start, start_multiproc_server

from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    SOURCE_TRANSFORMER_SOCK_PATH,
    MAX_THREADS,
    ServerType,
    _LOGGER,
    UDFType,
)

from pynumaflow.sourcetransformer._dtypes import SourceTransformCallable

from pynumaflow.shared import NumaflowServer


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
        server_type=ServerType.Sync,
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
        server_type: The type of server to be used
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size

        self.source_transform_instance = source_transform_instance
        self.server_type = server_type

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]
        if server_type == ServerType.Multiproc:
            self._server_options.append(("grpc.so_reuseport", 1))
            self._server_options.append(("grpc.so_reuseaddr", 1))

        # Set the number of processes to be spawned to the number of CPUs or
        # the value of the env var NUM_CPU_MULTIPROC defined by the user
        # Setting the max value to 2 * CPU count
        # Used for multiproc server
        self._process_count = min(
            int(os.getenv("NUM_CPU_MULTIPROC", str(os.cpu_count()))), 2 * os.cpu_count()
        )

    def start(self):
        """
        Starter function for the Source Transformer server,
        Handles the server type and starts the server.
        Currrently supported server types:
        1. ServerType.Sync
        2. ServerType.Multiproc
        """
        if self.server_type == ServerType.Sync:
            self.exec()
        elif self.server_type == ServerType.Multiproc:
            self.exec_multiproc()
        else:
            _LOGGER.error("Server type not supported - %s", str(self.server_type))
            raise NotImplementedError

    def exec(self):
        """
        Starts the Synchronous gRPC server on the given UNIX socket with given max threads.
        """
        transform_servicer = self.get_servicer(
            source_transform_instance=self.source_transform_instance, server_type=self.server_type
        )
        _LOGGER.info(
            "Sync GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )
        # Start the sync server
        sync_server_start(
            servicer=transform_servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_options=self._server_options,
            udf_type=UDFType.SourceTransformer,
        )

    def exec_multiproc(self):
        """
        Starts the Multiproc gRPC server on the given TCP sockets
        with given max threads.
        """
        transform_servicer = self.get_servicer(
            source_transform_instance=self.source_transform_instance, server_type=self.server_type
        )
        start_multiproc_server(
            max_threads=self.max_threads,
            servicer=transform_servicer,
            process_count=self._process_count,
            server_options=self._server_options,
            udf_type=UDFType.Map,
        )

    def get_servicer(
        self, source_transform_instance: SourceTransformCallable, server_type: ServerType
    ):
        """
        Returns the servicer instance for the given server type.
        """
        if server_type == ServerType.Sync:
            transform_servicer = SourceTransformer(handler=source_transform_instance)
        elif server_type == ServerType.Multiproc:
            transform_servicer = SourceTransformer(handler=source_transform_instance)
        return transform_servicer
