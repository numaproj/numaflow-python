from collections.abc import AsyncIterable

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.sinker._dtypes import Responses, Datum, Response
from pynumaflow.sinker._dtypes import SyncSinkCallable
from pynumaflow.proto.sinker import sink_pb2_grpc, sink_pb2
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER


async def datum_generator(
    request_iterator: AsyncIterable[sink_pb2.SinkRequest],
) -> AsyncIterable[Datum]:
    async for d in request_iterator:
        datum = Datum(
            keys=list(d.keys),
            sink_msg_id=d.id,
            value=d.value,
            event_time=d.event_time.ToDatetime(),
            watermark=d.watermark.ToDatetime(),
            headers=d.headers,
        )
        yield datum


class AsyncSinkServicer(sink_pb2_grpc.SinkServicer):
    """
    This class is used to create a new grpc Sink servicer instance.
    It implements the SinkServicer interface from the proto sink.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: SyncSinkCallable,
    ):
        self.__sink_handler: SyncSinkCallable = handler
        self.cleanup_coroutines = []

    async def SinkFn(
        self,
        request_iterator: AsyncIterable[sink_pb2.SinkRequest],
        context: NumaflowServicerContext,
    ) -> sink_pb2.SinkResponse:
        """
        Applies a sink function to a list of datum elements.
        The pascal case function name comes from the proto sink_pb2_grpc.py file.
        """
        # if there is an exception, we will mark all the responses as a failure
        datum_iterator = datum_generator(request_iterator=request_iterator)
        results = await self.__invoke_sink(datum_iterator)

        return sink_pb2.SinkResponse(results=results)

    async def __invoke_sink(self, datum_iterator: AsyncIterable[Datum]):
        try:
            rspns = await self.__sink_handler(datum_iterator)
        except Exception as err:
            err_msg = "UDSinkError: %r" % err
            _LOGGER.critical(err_msg, exc_info=True)
            rspns = Responses()
            async for _datum in datum_iterator:
                rspns.append(Response.as_failure(_datum.id, err_msg))
        responses = []
        for rspn in rspns:
            responses.append(
                sink_pb2.SinkResponse.Result(id=rspn.id, success=rspn.success, err_msg=rspn.err)
            )
        return responses

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> sink_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto sink_pb2_grpc.py file.
        """
        return sink_pb2.ReadyResponse(ready=True)
