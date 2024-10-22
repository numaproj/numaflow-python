from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.mapper._dtypes import Datum
from pynumaflow.mapper._dtypes import MapAsyncHandlerCallable, MapSyncCallable
from pynumaflow.proto.mapper import map_pb2, map_pb2_grpc
from pynumaflow.shared.server import exit_on_error
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER


class AsyncMapServicer(map_pb2_grpc.MapServicer):
    """
    This class is used to create a new grpc Async Map Servicer instance.
    It implements the SyncMapServicer interface from the proto map.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: MapAsyncHandlerCallable,
    ):
        self.__map_handler: MapSyncCallable = handler

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
                    headers=dict(request.headers),
                ),
                context,
            )
        except BaseException as e:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            exit_on_error(context, repr(e))
            return

        return map_pb2.MapResponse(results=res)

    async def __invoke_map(self, keys: list[str], req: Datum, context: NumaflowServicerContext):
        """
        Invokes the user defined function.
        """
        try:
            msgs = await self.__map_handler(keys, req)
        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            exit_on_error(context, repr(err))
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
