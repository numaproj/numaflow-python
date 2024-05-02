from collections.abc import AsyncIterable

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.mapstreamer import Datum
from pynumaflow.mapstreamer._dtypes import MapStreamCallable
from pynumaflow.proto.mapstreamer import mapstream_pb2_grpc, mapstream_pb2
from pynumaflow.shared.server import exit_on_error
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER


class AsyncMapStreamServicer(mapstream_pb2_grpc.MapStreamServicer):
    """
    This class is used to create a new grpc Map Stream Servicer instance.
    It implements the SyncMapServicer interface from the proto
    mapstream_pb2_grpc.py file.
    Provides the functionality for the required rpc methods.
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

        try:
            async for res in self.__invoke_map_stream(
                list(request.keys),
                Datum(
                    keys=list(request.keys),
                    value=request.value,
                    event_time=request.event_time.ToDatetime(),
                    watermark=request.watermark.ToDatetime(),
                    headers=dict(request.headers),
                ),
                context,
            ):
                yield mapstream_pb2.MapStreamResponse(result=res)
        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            exit_on_error(context, repr(err))
            return

    async def __invoke_map_stream(
        self, keys: list[str], req: Datum, context: NumaflowServicerContext
    ):
        try:
            async for msg in self.__map_stream_handler(keys, req):
                yield mapstream_pb2.MapStreamResponse.Result(
                    keys=msg.keys, value=msg.value, tags=msg.tags
                )
        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            exit_on_error(context, repr(err))
            raise err

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> mapstream_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto mapstream_pb2_grpc.py file.
        """
        return mapstream_pb2.ReadyResponse(ready=True)
