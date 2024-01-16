import os

from pynumaflow._constants import (
    MAX_THREADS,
    MAX_MESSAGE_SIZE,
    MAP_SOCK_PATH,
    UDFType,
    _PROCESS_COUNT,
)
from pynumaflow.mapper._dtypes import MapSyncCallable
from pynumaflow.mapper.servicer.sync_servicer import SyncMapServicer
from pynumaflow.shared.server import (
    NumaflowServer,
    start_multiproc_server,
)


class MapMultiprocServer(NumaflowServer):
    """
    Create a new grpc Multiproc Map Server instance.
    """

    def __init__(
        self,
        mapper_instance: MapSyncCallable,
        server_count: int = _PROCESS_COUNT,
        sock_path=MAP_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        """
        Create a new grpc Multiproc Map Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
        mapper_instance: The mapper instance to be used for Map UDF
        server_count: The number of grpc server instances to be forked for multiproc
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
            ("grpc.so_reuseport", 1),
            ("grpc.so_reuseaddr", 1),
        ]
        # Set the number of processes to be spawned to the number of CPUs or
        # the value of the env var NUM_CPU_MULTIPROC defined by the user
        # Setting the max value to 2 * CPU count
        # Used for multiproc server
        self._process_count = min(server_count, 2 * _PROCESS_COUNT)
        self.servicer = SyncMapServicer(handler=mapper_instance)

    def start(self) -> None:
        """
        Starts the N grpc servers gRPC serves on the with
        given max threads.
        where N = The number of CPUs or the
        value of the env var NUM_CPU_MULTIPROC defined by the user. The max value
        is set to 2 * CPU count.
        """

        # Start the multiproc server
        start_multiproc_server(
            max_threads=self.max_threads,
            servicer=self.servicer,
            process_count=self._process_count,
            server_options=self._server_options,
            udf_type=UDFType.Map,
        )
