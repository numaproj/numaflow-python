import os

from pynumaflow._constants import (
    MAX_THREADS,
    MAX_MESSAGE_SIZE,
    MAP_SOCK_PATH,
    UDFType,
    _PROCESS_COUNT,
    MAP_SERVER_INFO_FILE_PATH,
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
                        defaults to number of processors x4

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
                # To set the env server_count value set the env variable
                # NUM_CPU_MULTIPROC="N"
                server_count = int(os.getenv("NUM_CPU_MULTIPROC", "2"))
                prime_class = PrimeMap()
                # Server count is the number of server processes to start
                grpc_server = MapMultiprocServer(prime_class, server_count=server_count)
                grpc_server.start()

        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
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
        # the value of the env var NUM_CPU_MULTIPROC defined by the user
        # Setting the max value to 2 * CPU count
        # Used for multiproc server
        self._process_count = min(server_count, 2 * _PROCESS_COUNT)
        self.servicer = SyncMapServicer(handler=mapper_instance, multiproc=True)

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
            server_info_file=self.server_info_file,
            server_options=self._server_options,
            udf_type=UDFType.Map,
        )
