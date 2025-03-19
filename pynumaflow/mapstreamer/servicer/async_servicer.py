from collections.abc import AsyncIterable

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.mapstreamer import Datum
from pynumaflow.mapstreamer._dtypes import MapStreamCallable, MapStreamError
from pynumaflow.proto.mapper import map_pb2_grpc, map_pb2
from pynumaflow.shared.server import handle_async_error
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER, ERR_UDF_EXCEPTION_STRING


class AsyncMapStreamServicer(map_pb2_grpc.MapServicer):
    """
    This class is used to create a new grpc Map Stream Servicer instance.
    It implements the SyncMapServicer interface from the proto
    map_pb2_grpc.py file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: MapStreamCallable,
    ):
        self.__map_stream_handler: MapStreamCallable = handler

    async def MapFn(
        self,
        request_iterator: AsyncIterable[map_pb2.MapRequest],
        context: NumaflowServicerContext,
    ) -> AsyncIterable[map_pb2.MapResponse]:
        """
        Applies a map function to a datum stream in streaming mode.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        try:
            # The first message to be received should be a valid handshake
            req = await request_iterator.__anext__()
            # check if it is a valid handshake req
            if not (req.handshake and req.handshake.sot):
                raise MapStreamError("MapStreamFn: expected handshake as the first message")
            yield map_pb2.MapResponse(handshake=map_pb2.Handshake(sot=True))

            # read for each input request
            async for req in request_iterator:
                # yield messages as received from the UDF
                async for res in self.__invoke_map_stream(
                    list(req.request.keys),
                    Datum(
                        keys=list(req.request.keys),
                        value=req.request.value,
                        event_time=req.request.event_time.ToDatetime(),
                        watermark=req.request.watermark.ToDatetime(),
                        headers=dict(req.request.headers),
                    ),
                ):
                    yield map_pb2.MapResponse(results=[res], id=req.id)
                # send EOT to indicate end of transmission for a given message
                yield map_pb2.MapResponse(status=map_pb2.TransmissionStatus(eot=True), id=req.id)
        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            await handle_async_error(context, err, ERR_UDF_EXCEPTION_STRING)
            return

    async def __invoke_map_stream(self, keys: list[str], req: Datum):
        try:
            # Invoke the user handler for map stream
            async for msg in self.__map_stream_handler(keys, req):
                yield map_pb2.MapResponse.Result(keys=msg.keys, value=msg.value, tags=msg.tags)
        except BaseException as err:
            _LOGGER.critical("MapFn handler error", exc_info=True)
            raise err

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> map_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        return map_pb2.ReadyResponse(ready=True)
