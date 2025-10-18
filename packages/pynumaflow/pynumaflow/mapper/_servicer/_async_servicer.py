import asyncio
from collections.abc import AsyncIterable

from google.protobuf import empty_pb2 as _empty_pb2
from pynumaflow.shared.asynciter import NonBlockingIterator

from pynumaflow._constants import _LOGGER, STREAM_EOF, ERR_UDF_EXCEPTION_STRING
from pynumaflow.mapper._dtypes import MapAsyncCallable, Datum, MapError
from pynumaflow.proto.mapper import map_pb2, map_pb2_grpc
from pynumaflow.shared.server import handle_async_error
from pynumaflow.types import NumaflowServicerContext


class AsyncMapServicer(map_pb2_grpc.MapServicer):
    """
    This class is used to create a new grpc Async Map Servicer instance.
    It implements the SyncMapServicer interface from the proto map.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: MapAsyncCallable,
    ):
        self.background_tasks = set()
        self.__map_handler: MapAsyncCallable = handler

    async def MapFn(
        self,
        request_iterator: AsyncIterable[map_pb2.MapRequest],
        context: NumaflowServicerContext,
    ) -> AsyncIterable[map_pb2.MapResponse]:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        # proto repeated field(keys) is of type google._upb._message.RepeatedScalarContainer
        # we need to explicitly convert it to list
        try:
            # The first message to be received should be a valid handshake
            req = await request_iterator.__anext__()
            # check if it is a valid handshake req
            if not (req.handshake and req.handshake.sot):
                raise MapError("MapFn: expected handshake as the first message")
            yield map_pb2.MapResponse(handshake=map_pb2.Handshake(sot=True))

            global_result_queue = NonBlockingIterator()

            # reader task to process the input task and invoke the required tasks
            producer = asyncio.create_task(
                self._process_inputs(request_iterator, global_result_queue)
            )

            # keep reading on result queue and send messages back
            consumer = global_result_queue.read_iterator()
            async for msg in consumer:
                # If the message is an exception, we raise the exception
                if isinstance(msg, BaseException):
                    await handle_async_error(context, msg, ERR_UDF_EXCEPTION_STRING)
                    return
                # Send window response back to the client
                else:
                    yield msg
            # wait for the producer task to complete
            await producer
        except BaseException as e:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            await handle_async_error(context, e, ERR_UDF_EXCEPTION_STRING)
            return

    async def _process_inputs(
        self,
        request_iterator: AsyncIterable[map_pb2.MapRequest],
        result_queue: NonBlockingIterator,
    ):
        """
        Utility function for processing incoming MapRequests
        """
        try:
            # for each incoming request, create a background task to execute the
            # UDF code
            async for req in request_iterator:
                msg_task = asyncio.create_task(self._invoke_map(req, result_queue))
                # save a reference to a set to store active tasks
                self.background_tasks.add(msg_task)
                msg_task.add_done_callback(self.background_tasks.discard)

            # wait for all tasks to complete
            for task in self.background_tasks:
                await task

            # send an EOF to result queue to indicate that all tasks have completed
            await result_queue.put(STREAM_EOF)

        except BaseException:
            _LOGGER.critical("MapFn Error, re-raising the error", exc_info=True)

    async def _invoke_map(self, req: map_pb2.MapRequest, result_queue: NonBlockingIterator):
        """
        Invokes the user defined function.
        """
        try:
            datum = Datum(
                keys=list(req.request.keys),
                value=req.request.value,
                event_time=req.request.event_time.ToDatetime(),
                watermark=req.request.watermark.ToDatetime(),
                headers=dict(req.request.headers),
            )
            msgs = await self.__map_handler(list(req.request.keys), datum)
            datums = []
            for msg in msgs:
                datums.append(
                    map_pb2.MapResponse.Result(keys=msg.keys, value=msg.value, tags=msg.tags)
                )
            await result_queue.put(map_pb2.MapResponse(results=datums, id=req.id))
        except BaseException as err:
            _LOGGER.critical("MapFn handler error", exc_info=True)
            await result_queue.put(err)

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> map_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        return map_pb2.ReadyResponse(ready=True)
