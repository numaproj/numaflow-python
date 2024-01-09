import logging
import multiprocessing
import os


import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow.mapper._dtypes import Datum
from pynumaflow.mapper._dtypes import MapAsyncCallable, MapCallable
from pynumaflow.mapper.proto import map_pb2
from pynumaflow.mapper.proto import map_pb2_grpc
from pynumaflow.types import NumaflowServicerContext

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


class AsyncMapper(map_pb2_grpc.MapServicer):
    """
    Provides an interface to write an Async Mapper
    which will be exposed over gRPC.

    Args:
        handler: Function callable following the type signature of MapCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.mapper import Messages, Message\
    ...     Datum, AsyncMapper
    ... import aiorun
    ...
    >>> async def map_handler(key: [str], datum: Datum) -> Messages:
    ...   val = datum.value
    ...   _ = datum.event_time
    ...   _ = datum.watermark
    ...   messages = Messages(Message(val, keys=keys))
    ...   return messages
    ...
    >>> grpc_server = AsyncMapper(handler=map_handler)
    >>> aiorun.run(grpc_server.start())
    """

    def __init__(
        self,
        handler: MapAsyncCallable,
    ):
        self.__map_handler: MapCallable = handler

    async def MapFn(
        self, request: map_pb2.MapRequest, context: NumaflowServicerContext
    ) -> map_pb2.MapResponse:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        # proto repeated field(keys) is of type google._upb._message.RepeatedScalarContainer
        # we need to explicitly convert it to list
        try:
            res = await self.__invoke_map(
                list(request.keys),
                Datum(
                    keys=list(request.keys),
                    value=request.value,
                    event_time=request.event_time.ToDatetime(),
                    watermark=request.watermark.ToDatetime(),
                ),
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(e))
            return map_pb2.MapResponse(results=[])

        return map_pb2.MapResponse(results=res)

    async def __invoke_map(self, keys: list[str], req: Datum):
        """
        Invokes the user defined function.
        """
        try:
            msgs = await self.__map_handler(keys, req)
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            raise err
        datums = []
        for msg in msgs:
            datums.append(map_pb2.MapResponse.Result(keys=msg.keys, value=msg.value, tags=msg.tags))

        return datums

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> map_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        return map_pb2.ReadyResponse(ready=True)
