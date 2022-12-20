import asyncio
import logging
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Callable, Iterator

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow._constants import (
    FUNCTION_SOCK_PATH,
    DATUM_KEY,
    WIN_START_TIME,
    WIN_END_TIME,
    MAX_MESSAGE_SIZE,
)
from pynumaflow.function import Messages, Datum, IntervalWindow, Metadata
from pynumaflow.function.generated import udfunction_pb2
from pynumaflow.function.generated import udfunction_pb2_grpc
from pynumaflow.types import NumaflowServicerContext


if os.getenv("PYTHONDEBUG"):
    logging.basicConfig(level=logging.DEBUG)

_LOGGER = logging.getLogger(__name__)

UDFMapCallable = Callable[[str, Datum], Messages]
UDFReduceCallable = Callable[[str, Iterator[Datum], Metadata], Messages]
_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


class UserDefinedFunctionServicer(udfunction_pb2_grpc.UserDefinedFunctionServicer):
    """
    Provides an interface to write a User Defined Function (UDFunction)
    which will be exposed over gRPC.

    Args:
        map_handler: Function callable following the type signature of UDFMapCallable
        reduce_handler: Function callable following the type signature of UDFReduceCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.function import Messages, Message, \
    ...     Datum, Metadata, UserDefinedFunctionServicer
    ...
    >>> def map_handler(key: str, datum: Datum) -> Messages:
    ...   val = datum.value
    ...   _ = datum.event_time
    ...   _ = datum.watermark
    ...   messages = Messages(Message.to_vtx(key, val))
    ...   return messages
    ...
    >>> def reduce_handler(key: str, datums: Iterator[Datum], md: Metadata) -> Messages:
    ...   interval_window = md.interval_window
    ...   counter = 0
    ...   for _ in datums:
    ...     counter += 1
    ...   msg = (
    ...       f"counter:{counter} interval_window_start:{interval_window.start} "
    ...       f"interval_window_end:{interval_window.end}"
    ...   )
    ...   return Messages(Message.to_vtx(key, str.encode(msg)))
    ...
    >>> grpc_server = UserDefinedFunctionServicer(
    ...   map_handler=map_handler,
    ...   reduce_handler=reduce_handler,
    ... )
    >>> grpc_server.start()
    """

    def __init__(
        self,
        map_handler: UDFMapCallable = None,
        reduce_handler: UDFReduceCallable = None,
        sock_path=FUNCTION_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        if not (map_handler or reduce_handler):
            raise ValueError("Require a valid map handler and/or a valid reduce handler.")
        self.__map_handler: UDFMapCallable = map_handler
        self.__reduce_handler: UDFReduceCallable = reduce_handler
        self.sock_path = f"unix://{sock_path}"
        self._max_message_size = max_message_size
        self._max_threads = max_threads
        self._cleanup_coroutines = []

        self._server_options = [
            ("grpc.max_send_message_length", self._max_message_size),
            ("grpc.max_receive_message_length", self._max_message_size),
        ]

    def MapFn(
        self, request: udfunction_pb2.Datum, context: NumaflowServicerContext
    ) -> udfunction_pb2.DatumList:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the generated udfunction_pb2_grpc.py file.
        """
        key = ""
        for metadata_key, metadata_value in context.invocation_metadata():
            if metadata_key == DATUM_KEY:
                key = metadata_value

        try:
            msgs = self.__map_handler(
                key,
                Datum(
                    value=request.value,
                    event_time=request.event_time.event_time.ToDatetime(),
                    watermark=request.watermark.watermark.ToDatetime(),
                ),
            )
        except Exception as err:
            _LOGGER.critical("UDFError, dropping message on the floor: %r", err, exc_info=True)

            # a neat hack to drop
            msgs = Messages.as_forward_all(None)

        datums = []
        for msg in msgs.items():
            datums.append(udfunction_pb2.Datum(key=msg.key, value=msg.value))

        return udfunction_pb2.DatumList(elements=datums)

    def ReduceFn(
        self, request_iterator: Iterator[Datum], context: NumaflowServicerContext
    ) -> udfunction_pb2.DatumList:
        """
        Applies a reduce function to a datum stream.
        The pascal case function name comes from the generated udfunction_pb2_grpc.py file.
        """
        key, start, end = None, None, None
        for metadata_key, metadata_value in context.invocation_metadata():
            if metadata_key == DATUM_KEY:
                key = metadata_value
            elif metadata_key == WIN_START_TIME:
                start = metadata_value
            elif metadata_key == WIN_END_TIME:
                end = metadata_value
        if not (key or start or end):
            raise ValueError(
                f"Expected to have all key/window_start_time/window_end_time; "
                f"got key: {key}, start: {start}, end: {end}."
            )

        start_dt = datetime.fromtimestamp(int(start), timezone.utc)
        end_dt = datetime.fromtimestamp(int(end), timezone.utc)
        interval_window = IntervalWindow(start=start_dt, end=end_dt)

        try:
            msgs = self.__reduce_handler(
                key, request_iterator, Metadata(interval_window=interval_window)
            )
        except Exception as err:
            _LOGGER.critical("UDFError, dropping message on the floor: %r", err, exc_info=True)

            # a neat hack to drop
            msgs = Messages.as_forward_all(None)

        datums = []
        for msg in msgs.items():
            datums.append(udfunction_pb2.Datum(key=msg.key, value=msg.value))

        return udfunction_pb2.DatumList(elements=datums)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> udfunction_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the generated udfunction_pb2_grpc.py file.
        """
        return udfunction_pb2.ReadyResponse(ready=True)

    async def __serve_async(self, server) -> None:
        udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(
            UserDefinedFunctionServicer(self.__map_handler), server
        )
        server.add_insecure_port(self.sock_path)
        _LOGGER.info("GRPC Async Server listening on: %s", self.sock_path)
        await server.start()

        async def server_graceful_shutdown():
            """
            Shuts down the server with 5 seconds of grace period. During the
            grace period, the server won't accept new connections and allow
            existing RPCs to continue within the grace period.
            """
            _LOGGER.info("Starting graceful shutdown...")
            await server.stop(5)

        self._cleanup_coroutines.append(server_graceful_shutdown())
        await server.wait_for_termination()

    def start_async(self) -> None:
        """Starts the Async gRPC server on the given UNIX socket."""
        server = grpc.aio.server(
            ThreadPoolExecutor(max_workers=self._max_threads), options=self._server_options
        )
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self.__serve_async(server))
        finally:
            loop.run_until_complete(*self._cleanup_coroutines)
            loop.close()

    def start(self) -> None:
        """
        Starts the gRPC server on the given UNIX socket with given max threads.
        """
        server = grpc.server(
            ThreadPoolExecutor(max_workers=self._max_threads), options=self._server_options
        )
        udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(self, server)
        server.add_insecure_port(self.sock_path)
        server.start()
        _LOGGER.info(
            "GRPC Server listening on: %s with max threads: %s", self.sock_path, self._max_threads
        )
        server.wait_for_termination()
