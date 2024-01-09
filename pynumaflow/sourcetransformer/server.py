import logging
import os

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow import setup_logging
from pynumaflow.sourcetransformer import Datum
from pynumaflow.sourcetransformer._dtypes import SourceTransformCallable
from pynumaflow.proto.sourcetransformer import transform_pb2
from pynumaflow.proto.sourcetransformer import transform_pb2_grpc
from pynumaflow.types import NumaflowServicerContext

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)


class SourceTransformer(transform_pb2_grpc.SourceTransformServicer):
    """
    Provides an interface to write a Source Transformer
    which will be exposed over a Synchronous gRPC server.

    Args:
        handler: Function callable following the type signature of SourceTransformCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.sourcetransformer import Messages, Message \
    ...     Datum, SourceTransformer
    >>> def transform_handler(key: [str], datum: Datum) -> Messages:
    ...   val = datum.value
    ...   new_event_time = datetime.time()
    ...   _ = datum.watermark
    ...   message_t_s = Messages(Message(val, event_time=new_event_time, keys=key))
    ...   return message_t_s
    ...
    >>> grpc_server = SourceTransformer(handler=transform_handler)
    >>> grpc_server.start()
    """

    def __init__(
        self,
        handler: SourceTransformCallable,
    ):
        self.__transform_handler: SourceTransformCallable = handler

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
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
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
