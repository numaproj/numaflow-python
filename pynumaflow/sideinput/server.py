import os
from pynumaflow.shared import NumaflowServer
from pynumaflow.shared.server import sync_server_start
from pynumaflow.sideinput._dtypes import RetrieverCallable
from pynumaflow.sideinput.servicer.servicer import SideInputServicer
from pynumaflow._constants import (
    MAX_THREADS,
    MAX_MESSAGE_SIZE,
    SIDE_INPUT_SOCK_PATH,
    _LOGGER,
    UDFType,
    SIDE_INPUT_DIR_PATH,
)


class SideInputServer(NumaflowServer):
    """Server for side input"""

    def __init__(
        self,
        side_input_instance: RetrieverCallable,
        sock_path=SIDE_INPUT_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        side_input_dir_path=SIDE_INPUT_DIR_PATH,
    ):
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size

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
        # Start the server
        sync_server_start(
            servicer=side_input_servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_options=self._server_options,
            udf_type=UDFType.SideInput,
            add_info_server=False,
        )
