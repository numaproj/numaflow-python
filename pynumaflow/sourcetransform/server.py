import logging
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    SOURCE_TRANSFORMER_SOCK_PATH,
    MAX_MESSAGE_SIZE,
)
from pynumaflow.info.server import get_sdk_version, write as info_server_write
from pynumaflow.info.types import ServerInfo, Protocol, Language, SERVER_INFO_FILE_PATH
from pynumaflow.sourcetransform import Datum
from pynumaflow.sourcetransform._dtypes import SourceTransformCallable
from pynumaflow.sourcetransform.proto import transform_pb2
from pynumaflow.sourcetransform.proto import transform_pb2_grpc
from pynumaflow.types import NumaflowServicerContext

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


class SourceTransformer(transform_pb2_grpc.SourceTransformServicer):
    """
    Provides an interface to write a Source Transformer
    which will be exposed over a Synchronous gRPC server.

    Args:
        transform_handler: Function callable following the type signature of SourceTransformCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.sourcetransform import Messages, Message \
    ...     Datum, SourceTransformer

    >>> def transform_handler(key: [str], datum: Datum) -> Messages:
    ...   val = datum.value
    ...   new_event_time = datetime.time()
    ...   _ = datum.watermark
    ...   message_t_s = Messages(Message(val, event_time=new_event_time, keys=key))
    ...   return message_t_s
    ...
    >>> grpc_server = SourceTransformer(
    ...   transform_handler=transform_handler,
    ... )
    >>> grpc_server.start()
    """

    def __init__(
        self,
        transform_handler: SourceTransformCallable = None,
        sock_path=SOURCE_TRANSFORMER_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        if not transform_handler:
            raise ValueError("Require a source transform handler")

        self.__transform_handler: SourceTransformCallable = transform_handler
        self.sock_path = f"unix://{sock_path}"
        self._max_message_size = max_message_size
        self._max_threads = max_threads

        self._server_options = [
            ("grpc.max_send_message_length", self._max_message_size),
            ("grpc.max_receive_message_length", self._max_message_size),
        ]

    def SourceTransformFn(
        self, request: transform_pb2.SourceTransformRequest, context: NumaflowServicerContext
    ) -> transform_pb2.SourceTransformResponse:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the generated transform_pb2_grpc.py file.
        """

        # proto repeated field(keys) is of type google._upb._message.RepeatedScalarContainer
        # we need to explicitly convert it to list
        try:
            msgts = self.__transform_handler(
                list(request.keys),
                Datum(
                    keys=list(request.keys),
                    value=request.value,
                    event_time=request.event_time.ToDatetime(),
                    watermark=request.watermark.ToDatetime(),
                ),
            )
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error: %r", err, exc_info=True)
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(err))
            return transform_pb2.SourceTransformResponse(results=[])

        datums = []
        for msgt in msgts:
            event_time_timestamp = _timestamp_pb2.Timestamp()
            event_time_timestamp.FromDatetime(dt=msgt.event_time)
            datums.append(
                transform_pb2.SourceTransformResponse.Result(
                    keys=list(msgt.keys),
                    value=msgt.value,
                    tags=msgt.tags,
                    event_time=event_time_timestamp,
                )
            )
        return transform_pb2.SourceTransformResponse(results=datums)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> transform_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto transform_pb2_grpc.py file.
        """
        return transform_pb2.ReadyResponse(ready=True)

    def start(self) -> None:
        """
        Starts the gRPC server on the given UNIX socket with given max threads.
        """
        server = grpc.server(
            ThreadPoolExecutor(max_workers=self._max_threads), options=self._server_options
        )
        transform_pb2_grpc.add_SourceTransformServicer_to_server(self, server)
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
