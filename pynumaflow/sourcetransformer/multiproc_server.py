from pynumaflow.info.types import ServerInfo, MINIMUM_NUMAFLOW_VERSION, ContainerType
from pynumaflow.sourcetransformer.servicer._servicer import SourceTransformServicer

from pynumaflow.shared.server import start_multiproc_server

from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    SOURCE_TRANSFORMER_SOCK_PATH,
    NUM_THREADS_DEFAULT,
    UDFType,
    _PROCESS_COUNT,
    SOURCE_TRANSFORMER_SERVER_INFO_FILE_PATH,
    MAX_NUM_THREADS,
)

from pynumaflow.sourcetransformer._dtypes import SourceTransformCallable

from pynumaflow.shared import NumaflowServer


class SourceTransformMultiProcServer(NumaflowServer):
    """
    Class for a new Source Transformer Server instance.
    """

    def __init__(
        self,
        source_transform_instance: SourceTransformCallable,
        server_count: int = _PROCESS_COUNT,
        sock_path=SOURCE_TRANSFORMER_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=SOURCE_TRANSFORMER_SERVER_INFO_FILE_PATH,
    ):
        """
        Create a new grpc Source Transformer Multiproc Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
            source_transform_instance: The source transformer instance to be used for
            Source Transformer UDF
            sock_path: The UNIX socket path to be used for the server
            server_count: The number of grpc server instances to be forked for multiproc
            max_message_size: The max message size in bytes the server can receive and send
            max_threads: The max number of threads to be spawned;
                            defaults to 4 and max capped at 16

        Example invocation:
            import datetime
            import logging

            from pynumaflow.sourcetransformer import Messages, Message, Datum, SourceTransformServer

            # This is a simple User Defined Function example which receives a message,
            # applies the following
            # data transformation, and returns the message.
            # If the message event time is before year 2022, drop the message
            # with event time unchanged.
            # If it's within year 2022, update the tag to "within_year_2022" and
            # update the message event time to Jan 1st 2022.
            # Otherwise, (exclusively after year 2022), update the tag to
            # "after_year_2022" and update the


            january_first_2022 = datetime.datetime.fromtimestamp(1640995200)
            january_first_2023 = datetime.datetime.fromtimestamp(1672531200)


            def my_handler(keys: list[str], datum: Datum) -> Messages:
                val = datum.value
                event_time = datum.event_time
                messages = Messages()

                if event_time < january_first_2022:
                    logging.info("Got event time:%s, it is before 2022, so dropping", event_time)
                    messages.append(Message.to_drop(event_time))
                elif event_time < january_first_2023:
                    logging.info(
                        "Got event time:%s, it is within year 2022, so
                        forwarding to within_year_2022",
                        event_time,
                    )
                    messages.append(
                        Message(value=val, event_time=january_first_2022, tags=["within_year_2022"])
                    )
                else:
                    logging.info(
                        "Got event time:%s, it is after year 2022, so forwarding to
                        after_year_2022", event_time
                    )
                    messages.append(Message(value=val, event_time=january_first_2023,
                    tags=["after_year_2022"]))

                return messages

            if __name__ == "__main__":
                grpc_server = SourceTransformMultiProcServer(source_transform_instance=my_handler
                                                            ,server_count = 2)
                grpc_server.start()
        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self.source_transform_instance = source_transform_instance

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
        self.servicer = SourceTransformServicer(handler=source_transform_instance, multiproc=True)

    def start(self):
        """
        Starts the N grpc servers gRPC serves on the with
        given max threads.
        where N = The number of CPUs or the value of the parameter server_count
        defined by the user. The max value is capped to 2 * CPU count.
        """

        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[
            ContainerType.Sourcetransformer
        ]
        start_multiproc_server(
            max_threads=self.max_threads,
            servicer=self.servicer,
            process_count=self._process_count,
            server_info_file=self.server_info_file,
            server_options=self._server_options,
            udf_type=UDFType.SourceTransformer,
            server_info=serv_info,
        )
