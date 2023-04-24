import logging
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, AsyncIterable, List

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from pynumaflow.info.server import get_sdk_version, write as info_server_write
from pynumaflow.info.types import ServerInfo, Protocol, Language, SERVER_INFO_FILE_PATH

from pynumaflow import setup_logging
from pynumaflow._constants import (
    FUNCTION_SOCK_PATH,
    MAX_MESSAGE_SIZE,
)
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
_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


class Server(udfunction_pb2_grpc.UserDefinedFunctionServicer):
    """
    Provides an interface to write a User Defined Function (UDFunction)
    which will be exposed over a Synchronous gRPC server.

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
    ...     Datum, Metadata, Server
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
    >>> grpc_server = SyncServer(
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
        sock_path=FUNCTION_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        if not (map_handler or mapt_handler or reduce_handler):
            raise ValueError("Require a valid map/mapt handler and/or a valid reduce handler.")

        self.__map_handler: UDFMapCallable = map_handler
        self.__mapt_handler: UDFMapTCallable = mapt_handler
        self.__reduce_handler: UDFReduceCallable = reduce_handler
        self.sock_path = f"unix://{sock_path}"
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
        ]

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
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(err))
            return udfunction_pb2.DatumResponseList(elements=[])

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
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(err))
            return udfunction_pb2.DatumResponseList(elements=[])

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

    def ReduceFn(
        self,
        request_iterator: AsyncIterable[udfunction_pb2.DatumRequest],
        context: NumaflowServicerContext,
    ) -> udfunction_pb2.DatumResponseList:
        """
        This method is not implemented because we multiplex different keys
        on to a single stream and reduce requires a persistent connection.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        yield from ()

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> udfunction_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto udfunction_pb2_grpc.py file.
        """
        return udfunction_pb2.ReadyResponse(ready=True)

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
        serv_info = ServerInfo(
            protocol=Protocol.UDS,
            language=Language.PYTHON,
            version=get_sdk_version(),
        )
        info_server_write(server_info=serv_info, info_file=SERVER_INFO_FILE_PATH)
        _LOGGER.info(
            "GRPC Server listening on: %s with max threads: %s", self.sock_path, self._max_threads
        )
        server.wait_for_termination()
