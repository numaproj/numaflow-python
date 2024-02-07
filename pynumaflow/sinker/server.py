import os


from pynumaflow.sinker.servicer.sync_servicer import SyncSinkServicer

from pynumaflow._constants import (
    SINK_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    MAX_THREADS,
    _LOGGER,
    UDFType,
    SINK_SERVER_INFO_FILE_PATH,
)

from pynumaflow.shared.server import NumaflowServer, sync_server_start
from pynumaflow.sinker._dtypes import SyncSinkCallable


class SinkServer(NumaflowServer):
    """
    SinkServer is the main class to start a gRPC server for a sinker.
    """

    def __init__(
        self,
        sinker_instance: SyncSinkCallable,
        sock_path=SINK_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_info_file=SINK_SERVER_INFO_FILE_PATH,
    ):
        """
        Create a new grpc Sink Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
            sinker_instance: The sinker instance to be used for Sink UDF
            sock_path: The UNIX socket path to be used for the server
            max_message_size: The max message size in bytes the server can receive and send
            max_threads: The max number of threads to be spawned;
                            defaults to number of processors x4
        Example invocation:
            import os
            from collections.abc import Iterator

            from pynumaflow.sinker import Datum, Responses, Response, SinkServer
            from pynumaflow.sinker import Sinker
            from pynumaflow._constants import _LOGGER

            class UserDefinedSink(Sinker):
                def handler(self, datums: Iterator[Datum]) -> Responses:
                    responses = Responses()
                    for msg in datums:
                        _LOGGER.info("User Defined Sink %s", msg.value.decode("utf-8"))
                        responses.append(Response.as_success(msg.id))
                    return responses

            def udsink_handler(datums: Iterator[Datum]) -> Responses:
                responses = Responses()
                for msg in datums:
                    _LOGGER.info("User Defined Sink %s", msg.value.decode("utf-8"))
                    responses.append(Response.as_success(msg.id))
                return responses

            if __name__ == "__main__":
                invoke = os.getenv("INVOKE", "func_handler")
                if invoke == "class":
                    sink_handler = UserDefinedSink()
                else:
                    sink_handler = udsink_handler
                grpc_server = SinkServer(sink_handler)
                grpc_server.start()

        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, int(os.getenv("MAX_THREADS", "4")))
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self.sinker_instance = sinker_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.servicer = SyncSinkServicer(sinker_instance)

    def start(self):
        """
        Starts the Synchronous gRPC server on the
        given UNIX socket with given max threads.
        """
        _LOGGER.info(
            "Sync GRPC Sink listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )
        # Start the server
        sync_server_start(
            servicer=self.servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_info_file=self.server_info_file,
            server_options=self._server_options,
            udf_type=UDFType.Sink,
        )
