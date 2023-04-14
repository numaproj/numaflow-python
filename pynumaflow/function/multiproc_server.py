import contextlib
import logging
import multiprocessing
import os
import socket
from concurrent import futures
from typing import Callable, AsyncIterable, List

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
)
from pynumaflow._constants import MULTIPROC_FUNCTION_SOCK_PORT, MULTIPROC_FUNCTION_SOCK_ADDR
from pynumaflow.exceptions import SocketError
from pynumaflow.function import Messages, MessageTs, Datum, Metadata
from pynumaflow.function._dtypes import DatumMetadata
from pynumaflow.function.proto import udfunction_pb2
from pynumaflow.function.proto import udfunction_pb2_grpc
from pynumaflow.types import NumaflowServicerContext

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

UDFMapCallable = Callable[[List[str], Datum], Messages]
UDFMapTCallable = Callable[[List[str], Datum], MessageTs]
UDFReduceCallable = Callable[[List[str], AsyncIterable[Datum], Metadata], Messages]
_PROCESS_COUNT = int(os.getenv("NUM_CPU_MULTIPROC", multiprocessing.cpu_count()))
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


class MultiProcServer(udfunction_pb2_grpc.UserDefinedFunctionServicer):
    """
    Provides an interface to write a User Defined Function (UDFunction)
    which will be exposed over gRPC.

    Args:
        map_handler: Function callable following the type signature of UDFMapCallable
        mapt_handler: Function callable following the type signature of UDFMapTCallable
        reduce_handler: Function callable following the type signature of UDFReduceCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.function import Messages, Message, MessageTs, MessageT, \
    ...     Datum, Metadata, MultiProcServer
    ...
    >>> def map_handler(key: [str], datum: Datum) -> Messages:
    ...   val = datum.value
    ...   _ = datum.event_time
    ...   _ = datum.watermark
    ...   messages = Messages(Message(val, keys=keys))
    ...   return messages
    ...
    ... def reduce_handler(key: str, datums: Iterator[Datum], md: Metadata) -> Messages:
    ...           "Not supported"
    ...
    >>> def mapt_handler(key: [str], datum: Datum) -> MessageTs:
    ...   val = datum.value
    ...   new_event_time = datetime.time()
    ...   _ = datum.watermark
    ...   message_t_s = MessageTs(MessageT(val, event_time=new_event_time, keys=key))
    ...   return message_t_s
    ...
    ...
    >>> grpc_server = MultiProcServer(
    ...   mapt_handler=mapt_handler,
    ...   map_handler=map_handler,
    ... )
    >>> grpc_server.start()
    """

    def __init__(
        self,
        map_handler: UDFMapCallable = None,
        mapt_handler: UDFMapTCallable = None,
        reduce_handler: UDFReduceCallable = None,
        sock_path=MULTIPROC_FUNCTION_SOCK_PORT,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        if not (map_handler or mapt_handler or reduce_handler):
            raise ValueError("Require a valid map/mapt handler and/or a valid reduce handler.")

        self.__map_handler: UDFMapCallable = map_handler
        self.__mapt_handler: UDFMapTCallable = mapt_handler
        self.__reduce_handler: UDFReduceCallable = reduce_handler
        self._max_message_size = max_message_size
        self._max_threads = max_threads
        self.cleanup_coroutines = []
        # Collection for storing strong references to all running tasks.
        # Event loop only keeps a weak reference, which can cause it to
        # get lost during execution.
        self.background_tasks = set()

        self._server_options = [
            ("grpc.max_send_message_length", self._max_message_size),
            ("grpc.max_receive_message_length", self._max_message_size),
            ("grpc.so_reuseport", 1),
            ("grpc.so_reuseaddr", 1),
        ]
        self._sock_path = sock_path
        self._process_count = int(os.getenv("NUM_CPU_MULTIPROC", multiprocessing.cpu_count()))
        self._thread_concurrency = MAX_THREADS

    def MapFn(
        self, request: udfunction_pb2.DatumRequest, context: NumaflowServicerContext
    ) -> udfunction_pb2.DatumResponseList:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the proto udfunction_pb2_grpc.py file.
        """
        # proto repeated field(keys) is of type google._upb._message.RepeatedScalarContainer
        # we need to explicitly convert it to list
        try:
            msgs = self.__map_handler(
                list(request.keys),
                Datum(
                    keys=list(request.keys),
                    value=request.value,
                    event_time=request.event_time.event_time.ToDatetime(),
                    watermark=request.watermark.watermark.ToDatetime(),
                    metadata=DatumMetadata(
                        msg_id=request.metadata.id,
                        num_delivered=request.metadata.num_delivered,
                    ),
                ),
            )
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error: %r", err, exc_info=True)
            raise err

        datums = []

        for msg in msgs.items():
            datums.append(
                udfunction_pb2.DatumResponse(keys=msg.keys, value=msg.value, tags=msg.tags)
            )

        return udfunction_pb2.DatumResponseList(elements=datums)

    def MapTFn(
        self, request: udfunction_pb2.DatumRequest, context: NumaflowServicerContext
    ) -> udfunction_pb2.DatumResponseList:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the generated udfunction_pb2_grpc.py file.
        """

        # proto repeated field(keys) is of type google._upb._message.RepeatedScalarContainer
        # we need to explicitly convert it to list
        try:
            msgts = self.__mapt_handler(
                list(request.keys),
                Datum(
                    keys=list(request.keys),
                    value=request.value,
                    event_time=request.event_time.event_time.ToDatetime(),
                    watermark=request.watermark.watermark.ToDatetime(),
                    metadata=DatumMetadata(
                        msg_id=request.metadata.id,
                        num_delivered=request.metadata.num_delivered,
                    ),
                ),
            )
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error: %r", err, exc_info=True)
            raise err

        datums = []
        for msgt in msgts.items():
            event_time_timestamp = _timestamp_pb2.Timestamp()
            event_time_timestamp.FromDatetime(dt=msgt.event_time)
            watermark_timestamp = _timestamp_pb2.Timestamp()
            watermark_timestamp.FromDatetime(dt=request.watermark.watermark.ToDatetime())
            datums.append(
                udfunction_pb2.DatumResponse(
                    keys=list(msgt.keys),
                    value=msgt.value,
                    tags=msgt.tags,
                    event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
                    watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
                )
            )
        return udfunction_pb2.DatumResponseList(elements=datums)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> udfunction_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto udfunction_pb2_grpc.py file.
        """
        return udfunction_pb2.ReadyResponse(ready=True)

    def _run_server(self, bind_address):
        """Start a server in a subprocess."""
        _LOGGER.info("Starting new server.")
        server = grpc.server(
            futures.ThreadPoolExecutor(
                max_workers=self._thread_concurrency,
            ),
            options=self._server_options,
        )
        udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(self, server)
        server.add_insecure_port(bind_address)
        server.start()
        _LOGGER.info("GRPC Multi-Processor Server listening on: %s %d", bind_address, os.getpid())
        server.wait_for_termination()

    @contextlib.contextmanager
    def _reserve_port(self) -> int:
        """Find and reserve a port for all subprocesses to use."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR) == 0:
            raise SocketError("Failed to set SO_REUSEADDR.")
        if sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT) == 0:
            raise SocketError("Failed to set SO_REUSEPORT.")
        sock.bind(("", self._sock_path))
        try:
            yield sock.getsockname()[1]
        finally:
            sock.close()

    def start(self) -> None:
        """Start N grpc servers in different processes where N = CPU Count"""
        with self._reserve_port() as port:
            bind_address = f"{MULTIPROC_FUNCTION_SOCK_ADDR}:{port}"
            workers = []
            for _ in range(self._process_count):
                # NOTE: It is imperative that the worker subprocesses be forked before
                # any gRPC servers start up. See
                # https://github.com/grpc/grpc/issues/16001 for more details.
                worker = multiprocessing.Process(target=self._run_server, args=(bind_address,))
                worker.start()
                workers.append(worker)
            for worker in workers:
                worker.join()
