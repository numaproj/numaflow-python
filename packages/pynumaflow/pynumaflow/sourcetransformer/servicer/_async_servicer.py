import asyncio
from collections.abc import AsyncIterable

from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow._constants import _LOGGER, STREAM_EOF, ERR_UDF_EXCEPTION_STRING
from pynumaflow.proto.sourcetransformer import transform_pb2, transform_pb2_grpc
from pynumaflow.shared.asynciter import NonBlockingIterator
from pynumaflow.shared.server import handle_async_error
from pynumaflow.sourcetransformer import Datum
from pynumaflow.sourcetransformer._dtypes import SourceTransformAsyncCallable
from pynumaflow.types import NumaflowServicerContext


class SourceTransformAsyncServicer(transform_pb2_grpc.SourceTransformServicer):
    """
    This class is used to create a new grpc SourceTransform Async Servicer instance.
    It implements the SourceTransformServicer interface from the proto
    transform_pb2_grpc.py file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: SourceTransformAsyncCallable,
    ):
        self.background_tasks = set()
        self.__transform_handler: SourceTransformAsyncCallable = handler

    async def SourceTransformFn(
        self,
        request_iterator: AsyncIterable[transform_pb2.SourceTransformRequest],
        context: NumaflowServicerContext,
    ) -> AsyncIterable[transform_pb2.SourceTransformResponse]:
        """
        Applies a transform function to a SourceTransformRequest stream
        The pascal case function name comes from the proto transform_pb2_grpc.py file.
        """
        try:
            # The first message to be received should be a valid handshake
            req = await request_iterator.__anext__()
            # check if it is a valid handshake req
            if not (req.handshake and req.handshake.sot):
                raise Exception("SourceTransformFn: expected handshake message")
            yield transform_pb2.SourceTransformResponse(
                handshake=transform_pb2.Handshake(sot=True),
            )

            # result queue to stream messages from the user code back to the client
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
            _LOGGER.critical("SourceTransformFnError, re-raising the error", exc_info=True)
            await handle_async_error(context, e, ERR_UDF_EXCEPTION_STRING)
            return

    async def _process_inputs(
        self,
        request_iterator: AsyncIterable[transform_pb2.SourceTransformRequest],
        result_queue: NonBlockingIterator,
    ):
        """
        Utility function for processing incoming SourceTransformRequest
        """
        try:
            # for each incoming request, create a background task to execute the
            # UDF code
            async for req in request_iterator:
                msg_task = asyncio.create_task(self._invoke_transform(req, result_queue))
                # save a reference to a set to store active tasks
                self.background_tasks.add(msg_task)
                msg_task.add_done_callback(self.background_tasks.discard)

            # Wait for all tasks to complete concurrently
            await asyncio.gather(*self.background_tasks)

            # send an EOF to result queue to indicate that all tasks have completed
            await result_queue.put(STREAM_EOF)

        except BaseException:
            _LOGGER.critical("SourceTransformFnError Error, re-raising the error", exc_info=True)

    async def _invoke_transform(
        self, request: transform_pb2.SourceTransformRequest, result_queue: NonBlockingIterator
    ):
        """
        Invokes the user defined function.
        """
        try:
            datum = Datum(
                keys=list(request.request.keys),
                value=request.request.value,
                event_time=request.request.event_time.ToDatetime(),
                watermark=request.request.watermark.ToDatetime(),
                headers=dict(request.request.headers),
            )
            msgs = await self.__transform_handler(list(request.request.keys), datum)
            results = []
            for msg in msgs:
                event_time_timestamp = _timestamp_pb2.Timestamp()
                event_time_timestamp.FromDatetime(dt=msg.event_time)
                results.append(
                    transform_pb2.SourceTransformResponse.Result(
                        keys=list(msg.keys),
                        value=msg.value,
                        tags=msg.tags,
                        event_time=event_time_timestamp,
                    )
                )
            await result_queue.put(
                transform_pb2.SourceTransformResponse(results=results, id=request.request.id)
            )
        except BaseException as err:
            _LOGGER.critical("SourceTransformFnError handler error", exc_info=True)
            await result_queue.put(err)

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> transform_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto transform_pb2_grpc.py file.
        """
        return transform_pb2.ReadyResponse(ready=True)
