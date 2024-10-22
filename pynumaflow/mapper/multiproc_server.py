from pynumaflow._constants import (
    NUM_THREADS_DEFAULT,
    MAX_MESSAGE_SIZE,
    MAP_SOCK_PATH,
    UDFType,
    _PROCESS_COUNT,
    MAP_SERVER_INFO_FILE_PATH,
    MAX_NUM_THREADS,
)
from pynumaflow.info.server import get_metadata_env
from pynumaflow.info.types import (
    ServerInfo,
    METADATA_ENVS,
    MAP_MODE_KEY,
    MapMode,
    MULTIPROC_KEY,
    MINIMUM_NUMAFLOW_VERSION,
    ContainerType,
)
from pynumaflow.mapper._dtypes import MapSyncCallable
from pynumaflow.mapper._servicer._sync_servicer import SyncMapServicer
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
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=MAP_SERVER_INFO_FILE_PATH,
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
                        defaults to 4 and max capped at 16

        Example invocation:
            import math
            import os
            from pynumaflow.mapper import Messages, Message, Datum, Mapper, MapMultiprocServer

            def is_prime(n):
                for i in range(2, int(math.ceil(math.sqrt(n)))):
                    if n % i == 0:
                        return False
                else:
                    return True

            class PrimeMap(Mapper):
                def handler(self, keys: list[str], datum: Datum) -> Messages:
                    val = datum.value
                    _ = datum.event_time
                    _ = datum.watermark
                    messages = Messages()
                    for i in range(2, 100000):
                        is_prime(i)
                    messages.append(Message(val, keys=keys))
                    return messages

            if __name__ == "__main__":
                server_count = 2
                prime_class = PrimeMap()
                # Server count is the number of server processes to start
                grpc_server = MapMultiprocServer(prime_class, server_count=server_count)
                grpc_server.start()

        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self.mapper_instance = mapper_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
            ("grpc.so_reuseport", 1),
            ("grpc.so_reuseaddr", 1),
        ]
        # Set the number of processes to be spawned to the number of CPUs or
        # the value of the parameter server_count defined by the user
        # Setting the max value to 2 * CPU count
        # Used for multiproc server
        self._process_count = min(server_count, 2 * _PROCESS_COUNT)
        self.servicer = SyncMapServicer(handler=mapper_instance, multiproc=True)

    def start(self) -> None:
        """
        Starts the N grpc servers gRPC serves on the with
        given max threads.
        where N = The number of CPUs or the value of the parameter server_count
        defined by the user. The max value is capped to 2 * CPU count.
        """

        # Create the server info file
        server_info = ServerInfo.get_default_server_info()
        server_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Mapper]
        server_info.metadata = get_metadata_env(envs=METADATA_ENVS)
        # Add the MULTIPROC metadata using the number of servers to use
        server_info.metadata[MULTIPROC_KEY] = str(self._process_count)
        # Add the MAP_MODE metadata to the server info for the correct map mode
        server_info.metadata[MAP_MODE_KEY] = MapMode.UnaryMap

        # Start the multiproc server
        start_multiproc_server(
            max_threads=self.max_threads,
            servicer=self.servicer,
            process_count=self._process_count,
            server_info_file=self.server_info_file,
            server_options=self._server_options,
            udf_type=UDFType.Map,
            server_info=server_info,
        )
