import os


from pynumaflow.mapper.servicer.sync_servicer import SyncMapServicer

from pynumaflow._constants import (
    MAX_THREADS,
    MAX_MESSAGE_SIZE,
    _LOGGER,
    MAP_SOCK_PATH,
    UDFType,
)

from pynumaflow.mapper._dtypes import MapSyncCallable
from pynumaflow.shared.server import (
    NumaflowServer,
    sync_server_start,
)


class MapServer(NumaflowServer):
    """
    Create a new grpc Map Server instance.
    """

    def __init__(
        self,
        mapper_instance: MapSyncCallable,
        sock_path=MAP_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        """
        Create a new grpc Synchronous Map Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
        mapper_instance: The mapper instance to be used for Map UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size

        self.mapper_instance = mapper_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]
        # Get the servicer instance for the sync server
        self.servicer = SyncMapServicer(handler=mapper_instance)

    def start(self) -> None:
        """
        Starts the Synchronous gRPC server on the given UNIX socket with given max threads.
        """
        _LOGGER.info(
            "Sync GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )
        # Start the server
        sync_server_start(
            servicer=self.servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_options=self._server_options,
            udf_type=UDFType.Map,
        )
