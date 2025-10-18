from pynumaflow.info.types import ServerInfo, MINIMUM_NUMAFLOW_VERSION, ContainerType
from pynumaflow.shared import NumaflowServer
from pynumaflow.shared.server import sync_server_start
from pynumaflow.sideinput._dtypes import RetrieverCallable
from pynumaflow.sideinput.servicer.servicer import SideInputServicer
from pynumaflow._constants import (
    NUM_THREADS_DEFAULT,
    MAX_MESSAGE_SIZE,
    SIDE_INPUT_SOCK_PATH,
    _LOGGER,
    UDFType,
    SIDE_INPUT_DIR_PATH,
    SIDE_INPUT_SERVER_INFO_FILE_PATH,
    MAX_NUM_THREADS,
)


class SideInputServer(NumaflowServer):
    """
    Class for a new Side Input Server instance.
    Args:
        side_input_instance: The side input instance to be used for Side Input UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;

    Example invocation:
        import datetime
        from pynumaflow.sideinput import Response, SideInputServer, SideInput

        class ExampleSideInput(SideInput):
            def __init__(self):
                self.counter = 0

            def retrieve_handler(self) -> Response:
                time_now = datetime.datetime.now()
                # val is the value to be broadcasted
                val = f"an example: {str(time_now)}"
                self.counter += 1
                # broadcast every other time
                if self.counter % 2 == 0:
                    # no_broadcast_message() is used to indicate that there is no broadcast
                    return Response.no_broadcast_message()
                # broadcast_message() is used to indicate that there is a broadcast
                return Response.broadcast_message(val.encode("utf-8"))

        if __name__ == "__main__":
            grpc_server = SideInputServer(ExampleSideInput())
            grpc_server.start()

    """

    def __init__(
        self,
        side_input_instance: RetrieverCallable,
        sock_path=SIDE_INPUT_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        side_input_dir_path=SIDE_INPUT_DIR_PATH,
        server_info_file=SIDE_INPUT_SERVER_INFO_FILE_PATH,
    ):
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.side_input_instance = side_input_instance
        self.side_input_dir_path = side_input_dir_path
        self.servicer = SideInputServicer(side_input_instance)

    def start(self):
        """
        Starts the Synchronous gRPC server on the given UNIX socket with given max threads.
        """
        # Get the servicer instance based on the server type
        side_input_servicer = self.servicer

        _LOGGER.info(
            "Side Input GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )

        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Sideinput]
        # Start the server
        sync_server_start(
            servicer=side_input_servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_info_file=self.server_info_file,
            server_options=self._server_options,
            udf_type=UDFType.SideInput,
            server_info=serv_info,
        )
