from collections.abc import AsyncIterable

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.mapstreamer import Datum
from pynumaflow.mapstreamer._dtypes import MapStreamCallable
from pynumaflow.proto.mapstreamer import mapstream_pb2_grpc, mapstream_pb2
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER


async def datum_generator(
    request_iterator: AsyncIterable[mapstream_pb2.MapStreamRequest],
) -> AsyncIterable[Datum]:
    # i = 0
    async for d in request_iterator:
        # print(f"Loop {i} -- {d=}")
        # i += 1
        datum = Datum(
            keys=list(d.keys),
            value=d.value,
            event_time=d.event_time.ToDatetime(),
            watermark=d.watermark.ToDatetime(),
            headers=dict(d.headers),
        )
        yield datum


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
        async for res in self.__invoke_map_stream(
            list(request.keys),
            Datum(
                keys=list(request.keys),
                value=request.value,
                event_time=request.event_time.ToDatetime(),
                watermark=request.watermark.ToDatetime(),
                headers=dict(request.headers),
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

    async def MapStreamBatchFn(
        self,
        request_iterator: AsyncIterable[mapstream_pb2.MapStreamRequest],
        context: NumaflowServicerContext,
    ) -> AsyncIterable[mapstream_pb2.MapStreamResponse]:
        """
        Applies a sink function to a list of datum elements.
        The pascal case function name comes from the proto sink_pb2_grpc.py file.
        """
        datum_iterator = datum_generator(request_iterator=request_iterator)

        try:
            async for msg in self.__invoke_stream_batch(datum_iterator):
                yield msg
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            raise err

    async def __invoke_stream_batch(self, datum_iterator: AsyncIterable[Datum]):
        try:
            async for msg in self.__map_stream_handler.handler_stream(datum_iterator):
                yield mapstream_pb2.MapStreamResponse(
                    result=mapstream_pb2.MapStreamResponse.Result(
                        keys=msg.keys, value=msg.value, tags=msg.tags
                    )
                )
        except Exception as err:
            err_msg = "UDSinkError: %r" % err
            _LOGGER.critical(err_msg, exc_info=True)

            async for _datum in datum_iterator:
                yield mapstream_pb2.MapStreamResponse(
                    mapstream_pb2.MapStreamResponse.Result.as_failure(_datum.id, err_msg)
                )
