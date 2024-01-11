import os
from pynumaflow.shared import NumaflowServer
from pynumaflow.shared.server import sync_server_start
from pynumaflow.sideinput._dtypes import RetrieverCallable
from pynumaflow.sideinput.server import SideInput
from pynumaflow._constants import (
    MAX_THREADS,
    MAX_MESSAGE_SIZE,
    SIDE_INPUT_SOCK_PATH,
    ServerType,
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
        server_type=ServerType.Sync,
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
        self.server_type = server_type
        self.side_input_dir_path = side_input_dir_path

    def start(self):
        """Starter function for the server class, Handles the server type and
        starts the server accordingly. If the server type is not supported,
        raises NotImplementedError.
        Currently supported server types:
        1) ServerType.Sync
        """
        if self.server_type == ServerType.Sync:
            return self.exec()
        else:
            _LOGGER.error("Server type not supported - %s", str(self.server_type))
            raise NotImplementedError

    def exec(self):
        """
        Starts the Synchronous gRPC server on the given UNIX socket with given max threads.
        """
        # Get the servicer instance based on the server type
        side_input_servicer = self.get_servicer(
            side_input_instance=self.side_input_instance, server_type=self.server_type
        )
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

    def get_servicer(self, side_input_instance, server_type):
        """
        Returns the servicer instance based on the server type
        """
        if server_type == ServerType.Sync:
            return SideInput(side_input_instance)
