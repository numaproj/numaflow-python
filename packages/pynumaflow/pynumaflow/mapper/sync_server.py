from pynumaflow.info.types import (
    ServerInfo,
    MAP_MODE_KEY,
    MapMode,
    MINIMUM_NUMAFLOW_VERSION,
    ContainerType,
)
from pynumaflow.mapper._servicer._sync_servicer import SyncMapServicer

from pynumaflow._constants import (
    NUM_THREADS_DEFAULT,
    MAX_MESSAGE_SIZE,
    _LOGGER,
    MAP_SOCK_PATH,
    UDFType,
    MAP_SERVER_INFO_FILE_PATH,
    MAX_NUM_THREADS,
)

from pynumaflow.mapper._dtypes import MapSyncCallable
from pynumaflow.shared.server import (
    NumaflowServer,
    sync_server_start,
)


class MapServer(NumaflowServer):
    """
    Create a new grpc Map Server instance.
    Args:
        mapper_instance: The mapper instance to be used for Map UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to 4 and max capped at 16

    Example Invocation:
        from pynumaflow.mapper import Messages, Message, Datum, MapServer, Mapper

        class MessageForwarder(Mapper):
            def handler(self, keys: list[str], datum: Datum) -> Messages:
                val = datum.value
                _ = datum.event_time
                _ = datum.watermark
                return Messages(Message(value=val, keys=keys))

        def my_handler(keys: list[str], datum: Datum) -> Messages:
            val = datum.value
            _ = datum.event_time
            _ = datum.watermark
            return Messages(Message(value=val, keys=keys))


        if __name__ == "__main__":
            Use the class based approach or function based handler
            based on the env variable
            Both can be used and passed directly to the server class

            invoke = os.getenv("INVOKE", "func_handler")
            if invoke == "class":
                handler = MessageForwarder()
            else:
                handler = my_handler
            grpc_server = MapServer(handler)
            grpc_server.start()
    """

    def __init__(
        self,
        mapper_instance: MapSyncCallable,
        sock_path=MAP_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=MAP_SERVER_INFO_FILE_PATH,
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
                     defaults to 4 and max capped at 16
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

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

        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Mapper]
        # Add the MAP_MODE metadata to the server info for the correct map mode
        serv_info.metadata[MAP_MODE_KEY] = MapMode.UnaryMap
        # Start the server
        sync_server_start(
            servicer=self.servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_info_file=self.server_info_file,
            server_options=self._server_options,
            udf_type=UDFType.Map,
            server_info=serv_info,
        )
