import logging
import os

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    MAP_SOCK_PATH,
    MAX_THREADS,
)
from pynumaflow.mapper._dtypes import MapCallable
from pynumaflow.mapper.proto import map_pb2
from pynumaflow.mapper.proto import map_pb2_grpc
from pynumaflow.mapper.utils import _map_fn_util
from pynumaflow.types import NumaflowServicerContext

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)


class MultiProcMapper(map_pb2_grpc.MapServicer):
    """
    Provides an interface to write a Multi Proc Mapper
    which will be exposed over gRPC.

    Args:
        handler: Function callable following the type signature of MapCallable
        max_message_size: The max message size in bytes the server can receive and send

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.mapper import Messages, Message \
    ...     Datum, MultiProcMapper
    ...
    >>> def map_handler(keys: list[str], datum: Datum) -> Messages:
    ...   val = datum.value
    ...   _ = datum.event_time
    ...   _ = datum.watermark
    ...   messages = Messages(Message(val, keys=keys))
    ...   return messages
    ...
    >>> grpc_server = MultiProcMapper(handler=map_handler)
    >>> grpc_server.start()
    """

    __slots__ = (
        "__map_handler",
        "_max_message_size",
        "_server_options",
        "_process_count",
        "_threads_per_proc",
    )

    def __init__(
        self,
        handler: MapCallable,
        sock_path=MAP_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        self.__map_handler: MapCallable = handler
        self.sock_path = f"unix://{sock_path}"
        self._max_message_size = max_message_size
        self._max_threads = max_threads

        self._server_options = [
            ("grpc.max_send_message_length", self._max_message_size),
            ("grpc.max_receive_message_length", self._max_message_size),
            ("grpc.so_reuseport", 1),
            ("grpc.so_reuseaddr", 1),
        ]
        # Set the number of processes to be spawned to the number of CPUs or
        # the value of the env var NUM_CPU_MULTIPROC defined by the user
        # Setting the max value to 2 * CPU count
        self._process_count = min(
            int(os.getenv("NUM_CPU_MULTIPROC", str(os.cpu_count()))), 2 * os.cpu_count()
        )
        self._threads_per_proc = int(os.getenv("MAX_THREADS", "4"))

    def MapFn(
        self, request: map_pb2.MapRequest, context: NumaflowServicerContext
    ) -> map_pb2.MapResponse:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        return _map_fn_util(self.__map_handler, request, context)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> map_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        return map_pb2.ReadyResponse(ready=True)
