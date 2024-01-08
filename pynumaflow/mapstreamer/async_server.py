import logging
import multiprocessing
import os

from collections.abc import AsyncIterable

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    MAP_STREAM_SOCK_PATH,
)
from pynumaflow.mapstreamer import Datum
from pynumaflow.mapstreamer._dtypes import MapStreamCallable
from pynumaflow.mapstreamer.proto import mapstream_pb2
from pynumaflow.mapstreamer.proto import mapstream_pb2_grpc
from pynumaflow.types import NumaflowServicerContext

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


class AsyncMapStreamer(mapstream_pb2_grpc.MapStreamServicer):
    """
    Provides an interface to write a Map Streamer
    which will be exposed over gRPC.

    Args:
        handler: Function callable following the type signature of MapStreamCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.mapstreamer import Messages, Message \
    ...     Datum, AsyncMapStreamer
    ... import aiorun
    >>> async def map_stream_handler(key: [str], datums: Datum) -> AsyncIterable[Message]:
    ...    val = datum.value
    ...    _ = datum.event_time
    ...    _ = datum.watermark
    ...    for i in range(10):
    ...        yield Message(val, keys=keys)
    ...
    >>> grpc_server = AsyncMapStreamer(handler=map_stream_handler)
    >>> aiorun.run(grpc_server.start())
    """

    def __init__(
        self,
        handler: MapStreamCallable,
    ):
        self.__map_stream_handler: MapStreamCallable = handler

    async def MapStreamFn(
        self,
        request: mapstream_pb2.MapStreamRequest,
        context: NumaflowServicerContext,
    ) -> AsyncIterable[mapstream_pb2.MapStreamResponse]:
        """
        Applies a map function to a datum stream in streaming mode.
        The pascal case function name comes from the proto mapstream_pb2_grpc.py file.
        """

        async for res in self.__invoke_map_stream(
            list(request.keys),
            Datum(
                keys=list(request.keys),
                value=request.value,
                event_time=request.event_time.ToDatetime(),
                watermark=request.watermark.ToDatetime(),
            ),
        ):
            yield mapstream_pb2.MapStreamResponse(result=res)

    async def __invoke_map_stream(self, keys: list[str], req: Datum):
        try:
            async for msg in self.__map_stream_handler(keys, req):
                yield mapstream_pb2.MapStreamResponse.Result(
                    keys=msg.keys, value=msg.value, tags=msg.tags
                )
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            raise err

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> mapstream_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto mapstream_pb2_grpc.py file.
        """
        return mapstream_pb2.ReadyResponse(ready=True)
