import logging
import os

from collections.abc import AsyncIterable
from google.protobuf import timestamp_pb2 as _timestamp_pb2
import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow.sourcer._dtypes import ReadRequest
from pynumaflow.sourcer._dtypes import Offset, AckRequest, SourceCallable
from pynumaflow.proto.sourcer import source_pb2
from pynumaflow.proto.sourcer import source_pb2_grpc
from pynumaflow.types import NumaflowServicerContext

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)


class AsyncSourcer(source_pb2_grpc.SourceServicer):
    """
    Provides an interface to write an Asynchronous Sourcer
    which will be exposed over gRPC.

    Args:
        read_handler: Function callable following the type signature of AsyncSourceReadCallable
        ack_handler: Function handler for AckFn
        pending_handler: Function handler for PendingFn
        partitions_handler: Function handler for PartitionsFn

        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.sourcer import Message, get_default_partitions \
    ...     ReadRequest, AsyncSourcer,
    ... import aiorun
    ... async def read_handler(datum: ReadRequest) -> AsyncIterable[Message]:
    ...     payload = b"payload:test_mock_message"
    ...     keys = ["test_key"]
    ...     offset = mock_offset()
    ...     event_time = mock_event_time()
    ...     for i in range(10):
    ...         yield Message(payload=payload, keys=keys, offset=offset, event_time=event_time)
    ... async def ack_handler(ack_request: AckRequest):
    ...     return
    ... async def pending_handler() -> PendingResponse:
    ...     PendingResponse(count=10)
    ... async def partitions_handler() -> PartitionsResponse:
    ...     return PartitionsResponse(partitions=get_default_partitions())
    >>> grpc_server = AsyncSourcer(read_handler=read_handler,
    ...                     ack_handler=ack_handler,
    ...                     pending_handler=pending_handler,
    ...                     partitions_handler=partitions_handler)
    >>> aiorun.run(grpc_server.start())
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

        async for res in self.__invoke_source_read_stream(
            ReadRequest(
                num_records=request.request.num_records,
                timeout_in_ms=request.request.timeout_in_ms,
            )
        ):
            yield source_pb2.ReadResponse(result=res)

    async def __invoke_source_read_stream(self, req: ReadRequest):
        try:
            async for msg in self.__source_read_handler(req):
                event_time_timestamp = _timestamp_pb2.Timestamp()
                event_time_timestamp.FromDatetime(dt=msg.event_time)
                yield source_pb2.ReadResponse.Result(
                    payload=msg.payload,
                    keys=msg.keys,
                    offset=msg.offset.as_dict,
                    event_time=event_time_timestamp,
                )
        except Exception as err:
            _LOGGER.critical("User-Defined Source ReadError ", exc_info=True)
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
            await self.__invoke_ack(ack_req=offsets)
        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(e))
            raise e

        return source_pb2.AckResponse()

    async def __invoke_ack(self, ack_req: list[Offset]):
        """
        Invokes the Source Ack Function.
        """
        try:
            await self.__source_ack_handler(AckRequest(offsets=ack_req))
        except Exception as err:
            _LOGGER.critical("AckFn Error", exc_info=True)
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
        except Exception as err:
            _LOGGER.critical("PendingFn Error", exc_info=True)
            raise err
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
        except Exception as err:
            _LOGGER.critical("PartitionsFn Error", exc_info=True)
            raise err
        resp = source_pb2.PartitionsResponse.Result(partitions=partitions.partitions)
        return source_pb2.PartitionsResponse(result=resp)