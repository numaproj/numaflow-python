import logging
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    SIDE_INPUT_SOCK_PATH,
)
from pynumaflow.sideinput import Response
from pynumaflow.sideinput.proto import sideinput_pb2, sideinput_pb2_grpc
from pynumaflow.types import NumaflowServicerContext

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

RetrieverCallable = Callable[[], Response]
_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


class SideInput(sideinput_pb2_grpc.SideInputServicer):
    """
    Provides an interface to write a User Defined Side Input (UDSideInput)
    which will be exposed over gRPC.

    Args:
        handler: Function callable following the type signature of RetrieverCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x 4

    Example invocation:
    >>> from typing import List
    >>> from pynumaflow.sideinput import Response, SideInput
    >>> def my_handler() -> Response:
    ...   response = Response.broadcast_message(b"hello")
    ...   return response
    >>> grpc_server = SideInput(my_handler)
    >>> grpc_server.start()
    """

    SIDE_INPUT_DIR_PATH = "/var/numaflow/side-inputs"

    def __init__(
        self,
        handler: RetrieverCallable,
        sock_path=SIDE_INPUT_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        self.__retrieve_handler: RetrieverCallable = handler
        self.sock_path = f"unix://{sock_path}"
        self._max_message_size = max_message_size
        self._max_threads = max_threads
        self.cleanup_coroutines = []

        self._server_options = [
            ("grpc.max_send_message_length", self._max_message_size),
            ("grpc.max_receive_message_length", self._max_message_size),
        ]

    def RetrieveSideInput(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> sideinput_pb2.SideInputResponse:
        """
        Applies a sideinput function for a retrieval request.
        The pascal case function name comes from the proto sideinput_pb2_grpc.py file.
        """
        # if there is an exception, we will mark all the responses as a failure
        try:
            rspn = self.__retrieve_handler()
        except Exception as err:
            err_msg = "RetrieveSideInputErr: %r" % err
            _LOGGER.critical(err_msg, exc_info=True)
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(err))
            return sideinput_pb2.SideInputResponse(value=None, no_broadcast=True)

        return sideinput_pb2.SideInputResponse(value=rspn.value, no_broadcast=rspn.no_broadcast)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> sideinput_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto sideinput_pb2_grpc.py file.
        """
        return sideinput_pb2.ReadyResponse(ready=True)

    def start(self) -> None:
        """
        Starts the gRPC server on the given UNIX socket with given max threads.
        """
        server = grpc.server(
            ThreadPoolExecutor(max_workers=self._max_threads), options=self._server_options
        )
        sideinput_pb2_grpc.add_SideInputServicer_to_server(
            SideInput(self.__retrieve_handler), server
        )
        server.add_insecure_port(self.sock_path)
        server.start()
        _LOGGER.info(
            "Side Input gRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self._max_threads,
        )
        server.wait_for_termination()
