from collections.abc import AsyncIterable
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.shared.server import exit_on_error
from pynumaflow.sourcer._dtypes import ReadRequest
from pynumaflow.sourcer._dtypes import Offset, AckRequest, SourceCallable
from pynumaflow.proto.sourcer import source_pb2
from pynumaflow.proto.sourcer import source_pb2_grpc
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER


class AsyncSourceServicer(source_pb2_grpc.SourceServicer):
    """
    This class is used to create a new grpc Source servicer instance.
    It implements the SourceServicer interface from the proto source.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(self, source_handler: SourceCallable):
        self.source_handler = source_handler
        self.__source_read_handler = source_handler.read_handler
        self.__source_ack_handler = source_handler.ack_handler
        self.__source_pending_handler = source_handler.pending_handler
        self.__source_partitions_handler = source_handler.partitions_handler
        self.cleanup_coroutines = []

    async def ReadFn(
        self,
        request: source_pb2.ReadRequest,
        context: NumaflowServicerContext,
    ) -> AsyncIterable[source_pb2.ReadResponse]:
        """
        Applies a Read function and returns a stream of datum responses.
        The pascal case function name comes from the proto source_pb2_grpc.py file.
        """
        try:
            async for res in self.__invoke_source_read_stream(
                ReadRequest(
                    num_records=request.request.num_records,
                    timeout_in_ms=request.request.timeout_in_ms,
                ),
                context,
            ):
                yield source_pb2.ReadResponse(result=res)
        except BaseException as err:
            _LOGGER.critical("User-Defined Source ReadError ", exc_info=True)
            exit_on_error(context, str(err))
            return

    async def __invoke_source_read_stream(self, req: ReadRequest, context: NumaflowServicerContext):
        try:
            async for msg in self.__source_read_handler(req):
                event_time_timestamp = _timestamp_pb2.Timestamp()
                event_time_timestamp.FromDatetime(dt=msg.event_time)
                yield source_pb2.ReadResponse.Result(
                    payload=msg.payload,
                    keys=msg.keys,
                    offset=msg.offset.as_dict,
                    event_time=event_time_timestamp,
                    headers=msg.headers,
                )
        except BaseException as err:
            _LOGGER.critical("User-Defined Source ReadError ", exc_info=True)
            exit_on_error(context, repr(err))
            raise err

    async def AckFn(
        self, request: source_pb2.AckRequest, context: NumaflowServicerContext
    ) -> source_pb2.AckResponse:
        """
        Applies an Ack function in User Defined Source
        """
        # proto repeated field(offsets) is of type google._upb._message.RepeatedScalarContainer
        # we need to explicitly convert it to list
        offsets = []
        for offset in request.request.offsets:
            offsets.append(Offset(offset.offset, offset.partition_id))
        try:
            await self.__invoke_ack(ack_req=offsets, context=context)
        except BaseException as e:
            _LOGGER.critical("AckFn Error", exc_info=True)
            exit_on_error(context, repr(e))
            return

        return source_pb2.AckResponse()

    async def __invoke_ack(self, ack_req: list[Offset], context: NumaflowServicerContext):
        """
        Invokes the Source Ack Function.
        """
        try:
            await self.__source_ack_handler(AckRequest(offsets=ack_req))
        except BaseException as err:
            _LOGGER.critical("AckFn Error", exc_info=True)
            exit_on_error(context, repr(err))
            raise err
        return source_pb2.AckResponse.Result()

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> source_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto source_pb2_grpc.py file.
        """
        return source_pb2.ReadyResponse(ready=True)

    async def PendingFn(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> source_pb2.PendingResponse:
        """
        PendingFn returns the number of pending records
        at the user defined source.
        """
        try:
            count = await self.__source_pending_handler()
        except BaseException as err:
            _LOGGER.critical("PendingFn Error", exc_info=True)
            exit_on_error(context, repr(err))
            return
        resp = source_pb2.PendingResponse.Result(count=count.count)
        return source_pb2.PendingResponse(result=resp)

    async def PartitionsFn(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> source_pb2.PartitionsResponse:
        """
        PartitionsFn returns the partitions of the user defined source.
        """
        try:
            partitions = await self.__source_partitions_handler()
        except BaseException as err:
            _LOGGER.critical("PartitionsFn Error", exc_info=True)
            exit_on_error(context, repr(err))
            return
        resp = source_pb2.PartitionsResponse.Result(partitions=partitions.partitions)
        return source_pb2.PartitionsResponse(result=resp)
