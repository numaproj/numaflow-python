import asyncio
from collections import deque
from collections.abc import AsyncIterable
from datetime import datetime

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.batchmapper._dtypes import Datum, Message, Messages
from pynumaflow.batchmapper._dtypes import MapBatchAsyncHandlerCallable
from pynumaflow.proto.batchmapper import batchmap_pb2, batchmap_pb2_grpc
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER


async def datum_generator(
    request_iterator: AsyncIterable[batchmap_pb2.BatchMapRequest],
) -> AsyncIterable[Datum]:
    async for d in request_iterator:
        datum = Datum(
            id=d.id,
            keys=list(d.keys),
            value=d.value,
            event_time=d.event_time.ToDatetime(),
            watermark=d.watermark.ToDatetime(),
            headers=dict(d.headers),
        )
        yield datum


class BatchMapServicer(batchmap_pb2_grpc.BatchMapServicer):
    """
    This class is used to create a new grpc Batch Map Servicer instance.
    It implements the SyncMapServicer interface from the proto batch map.proto file.

    This is the core servicer and the default implementation will function on a handler
    taking an async iterable to handle messages as deemed fit by the handler. The incoming
    data is unbounded at the SDK level is is limited by message retrieval semantics on
    numa sidecar, so the handler must be aware of how to appropriately manage number of
    messages in-flight at once.

    This class is designed to enable different access patterns via inheritance. This can
    be useful to build consistent data access ability across different applications.
    In order to use this, the class being used must be provided to the BatchMapAsyncServer
    for initialization and override present_data

    Example Override:
    class OtherImpl(BaseMapServicer):
        async def present_data(self, datum_iterator: AsyncIterable[Datum])
            -> AsyncIterable[batchmap_pb2.BatchMapResponse]:
            ... # manage data here

    """

    def __init__(
        self,
        handler: MapBatchAsyncHandlerCallable,
    ):
        self._handler: MapBatchAsyncHandlerCallable = handler

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> batchmap_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto batchmap_pb2_grpc.py file.
        """
        return batchmap_pb2.ReadyResponse(ready=True)

    async def BatchMapFn(
        self,
        request_iterator: AsyncIterable[batchmap_pb2.BatchMapRequest],
        context: NumaflowServicerContext,
    ) -> AsyncIterable[batchmap_pb2.BatchMapResponse]:
        """
        Applies a batch function to a list of datum elements.
        The pascal case function name comes from the proto batchmap_pb2_grpc.py file.
        """
        datum_iterator = datum_generator(request_iterator=request_iterator)

        try:
            # async for msg in self.__invoke_stream_batch(datum_iterator):
            async for msg in self.present_data(datum_iterator):
                yield msg
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            raise err

    async def present_data(
        self, datum_iterator: AsyncIterable[Datum]
    ) -> AsyncIterable[batchmap_pb2.BatchMapResponse]:
        try:
            async for response_msgs in self._handler(datum_iterator):
                # Implicitly ensure we have a list
                yield batchmap_pb2.BatchMapResponse(
                    results=[
                        batchmap_pb2.BatchMapResponse.Result(
                            keys=msg.keys, value=msg.value, tags=msg.tags
                        )
                        for msg in response_msgs.messages
                    ],
                    id=response_msgs.id,
                )
        except Exception as err:
            err_msg = "UDSinkError: %r" % err
            _LOGGER.critical(err_msg, exc_info=True)

            async for _datum in datum_iterator:
                yield batchmap_pb2.BatchMapResponse(
                    batchmap_pb2.BatchMapResponse.Result.as_failure(_datum.id, err_msg)
                )


# ----
class BatchMapUnaryServicer(BatchMapServicer):
    """Operates a async-mapper unary style prentation. Does not provide batching,
    but a streamlined interface for individual message handling that can be used in place of `mapper.MapAsyncServer`
    """

    async def present_data(
        self,
        datum_iterator: AsyncIterable[Datum],
    ) -> AsyncIterable[batchmap_pb2.BatchMapResponse]:
        try:
            async for msg in datum_iterator:
                async for to_ret in self._process_one_flatmap(msg):
                    yield to_ret
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            raise err

    async def _process_one_flatmap(self, msg: Datum):
        msg_id = msg.id

        try:
            # async for result in self._map_stream_handler.handler_stream(msg):
            result = await self._handler(msg.keys, msg)
            if isinstance(result, Message):
                result = Messages(result)

            yield batchmap_pb2.BatchMapResponse(
                results=[
                    batchmap_pb2.BatchMapResponse.Result(
                        keys=msg.keys, value=msg.value, tags=msg.tags
                    )
                    for msg in result
                ],
                id=msg_id,
            )

        except Exception as err:
            err_msg = "UDFError, re-raising the error: %r" % err
            _LOGGER.critical(err_msg, exc_info=True)
            raise err


async def _make_async_iter(iterable):
    for item in iterable:
        yield item


class BatchMapGroupingServicer(BatchMapServicer):
    def __init__(
        self, handler: MapBatchAsyncHandlerCallable, max_batch_size: int = 10, timeout_sec: int = 5
    ):
        super().__init__(handler)

        self._max_batch_size = max_batch_size
        self._timeout_sec = timeout_sec

    async def present_data(
        self,
        datum_iterator: AsyncIterable[Datum],
    ) -> AsyncIterable[Message]:
        buffer: deque[Datum] = deque()
        start_time = datetime.now()

        keep_going = True

        async def fetch_next_datum():
            nonlocal keep_going
            try:
                return await asyncio.wait_for(datum_iterator.__anext__(), self._timeout_sec)
            except asyncio.TimeoutError:
                return None
            except StopAsyncIteration:
                keep_going = False
                return None

        while keep_going:
            datum = await fetch_next_datum()
            if datum:
                buffer.append(datum)
                if len(buffer) >= self._max_batch_size:
                    async for message in self._process_stream_map(buffer):
                        yield message
                    buffer.clear()
                    start_time = datetime.now()
            else:
                if buffer:
                    async for message in self._process_stream_map(buffer):
                        yield message
                    buffer.clear()
                    start_time = datetime.now()

            # Check if the timeout has been exceeded
            if (datetime.now() - start_time).total_seconds() >= self._timeout_sec:
                if buffer:
                    async for message in self._process_stream_map(buffer):
                        yield message
                    buffer.clear()
                    start_time = datetime.now()

    async def _process_stream_map(self, msgs: list[Datum]):
        try:
            async for response_msgs in self._handler(_make_async_iter(msgs)):
                # Implicitly ensure we have a list
                yield batchmap_pb2.BatchMapResponse(
                    results=[
                        batchmap_pb2.BatchMapResponse.Result(
                            keys=msg.keys, value=msg.value, tags=msg.tags
                        )
                        for msg in response_msgs.messages
                    ],
                    id=response_msgs.id,
                )
        except Exception as err:
            err_msg = "UDSinkError: %r" % err
            _LOGGER.critical(err_msg, exc_info=True)
            try:
                for _datum in msgs:
                    yield batchmap_pb2.BatchMapResponse(
                        batchmap_pb2.BatchMapResponse.Result.as_failure(_datum.id, err_msg)
                    )
            except Exception as ex:
                print(f"{ex=}")
