import asyncio
from collections.abc import AsyncIterable

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.shared.asynciter import NonBlockingIterator
from pynumaflow._constants import _LOGGER, STREAM_EOF, ERR_UDF_EXCEPTION_STRING
from pynumaflow.mapstreamer import Datum
from pynumaflow.mapstreamer._dtypes import MapStreamCallable, MapStreamError
from pynumaflow.proto.mapper import map_pb2_grpc, map_pb2
from pynumaflow.shared.server import update_context_err
from pynumaflow.types import NumaflowServicerContext


class AsyncMapStreamServicer(map_pb2_grpc.MapServicer):
    """
    Concurrent gRPC Map Stream Servicer.
    Spawns one background task per incoming MapRequest; each task streams
    results as produced and finally emits an EOT for that request.
    """

    def __init__(self, handler: MapStreamCallable):
        self.__map_stream_handler: MapStreamCallable = handler
        self._background_tasks: set[asyncio.Task] = set()
        self._shutdown_event: asyncio.Event | None = None
        self._error: BaseException | None = None

    def set_shutdown_event(self, event: asyncio.Event):
        """Wire up the shutdown event created by the server's aexec() coroutine."""
        self._shutdown_event = event

    async def MapFn(
        self,
        request_iterator: AsyncIterable[map_pb2.MapRequest],
        context: NumaflowServicerContext,
    ) -> AsyncIterable[map_pb2.MapResponse]:
        """
        Applies a map function to a datum stream in streaming mode.
        The PascalCase name comes from the generated map_pb2_grpc.py file.
        """
        try:
            # First message must be a handshake
            first = await request_iterator.__anext__()
            if not (first.handshake and first.handshake.sot):
                raise MapStreamError("MapStreamFn: expected handshake as the first message")
            # Acknowledge handshake
            yield map_pb2.MapResponse(handshake=map_pb2.Handshake(sot=True))

            # Global non-blocking queue for outbound responses / errors
            global_result_queue = NonBlockingIterator()

            # Start producer that turns each inbound request into a background task
            producer = asyncio.create_task(
                self._process_inputs(request_iterator, global_result_queue)
            )

            # Consume results as they arrive and stream them to the client
            async for msg in global_result_queue.read_iterator():
                if isinstance(msg, BaseException):
                    err_msg = f"{ERR_UDF_EXCEPTION_STRING}: {repr(msg)}"
                    _LOGGER.critical(err_msg, exc_info=True)
                    update_context_err(context, msg, err_msg)
                    self._error = msg
                    if self._shutdown_event is not None:
                        self._shutdown_event.set()
                    return
                else:
                    # msg is a map_pb2.MapResponse, already formed
                    yield msg

            # Ensure producer has finished (covers graceful shutdown)
            await producer

        except asyncio.CancelledError:
            # Task cancelled during shutdown (e.g. SIGTERM) — not a UDF fault.
            _LOGGER.info("Server shutting down, cancelling RPC.")
            if self._shutdown_event is not None:
                self._shutdown_event.set()
            return

        except BaseException as e:
            err_msg = f"{ERR_UDF_EXCEPTION_STRING}: {repr(e)}"
            _LOGGER.critical(err_msg, exc_info=True)
            update_context_err(context, e, err_msg)
            self._error = e
            if self._shutdown_event is not None:
                self._shutdown_event.set()
            return

    async def _process_inputs(
        self,
        request_iterator: AsyncIterable[map_pb2.MapRequest],
        result_queue: NonBlockingIterator,
    ) -> None:
        """
        Reads MapRequests from the client and spawns a background task per request.
        Each task streams results to result_queue as they are produced.
        """
        try:
            async for req in request_iterator:
                task = asyncio.create_task(self._invoke_map_stream(req, result_queue))
                self._background_tasks.add(task)
                # Remove from the set when done to avoid memory growth
                task.add_done_callback(self._background_tasks.discard)

            # Wait for all in-flight tasks to complete
            if self._background_tasks:
                await asyncio.gather(*list(self._background_tasks), return_exceptions=False)

            # Signal end-of-stream to the consumer
            await result_queue.put(STREAM_EOF)

        except BaseException as e:
            _LOGGER.critical("MapFn Error, re-raising the error", exc_info=True)
            # Surface the error to the consumer; MapFn will handle and close the RPC
            await result_queue.put(e)

    async def _invoke_map_stream(
        self,
        req: map_pb2.MapRequest,
        result_queue: NonBlockingIterator,
    ) -> None:
        """
        Invokes the user-provided async generator for a single request and
        pushes each result onto the global queue, followed by an EOT for this id.
        """
        try:
            datum = Datum(
                keys=list(req.request.keys),
                value=req.request.value,
                event_time=req.request.event_time.ToDatetime(),
                watermark=req.request.watermark.ToDatetime(),
                headers=dict(req.request.headers),
            )

            # Stream results from the user handler as they are produced.
            # The asyncio.sleep(0) after each put yields control to the event loop,
            # allowing MapFn to consume and stream the response to gRPC immediately.
            # Without it, await Queue.put() on an unbounded queue completes without
            # suspending (Queue.full() is always False), starving other tasks.
            # The starvation can happen if the UDF code yields messages using regular
            # for-loop (non async). See the sample code in https://github.com/numaproj/numaflow-python/issues/342
            # With asyncio.sleep(0), this makes our below 'async for' loop equivalent to:
            #
            #   while True:
            #       msg = await handler.__anext__()  # await point
            #       await result_queue.put(...)
            #
            # The "await result_queue.put()" isn't a real await point yielding control back to
            # eventloop in the case of an unbounded queue. When queue is not full, it simply calls
            # a non-async function https://github.com/python/cpython/blob/f4c9bc899b982b9742b45cff0643fa34de3dc84d/Lib/asyncio/queues.py#L125-L154
            # Or you can refer the source code with:
            # python -c "import asyncio, inspect; print(inspect.getsource(asyncio.Queue.put))"
            # This results in a tight loop, blocking other tasks on event loop from proceeding.
            # Like in the issue linked here, if the user yields 10 messages at 1 second a part,
            # the task that reads from the queue can only proceed this 'async for loop' ends as
            # it never yields control back to eventloop. So you will see all 10 messages at the
            # same time in the next vertex instead of in a true streaming fashion.
            # The asyncio.sleep(0) will yield the control back to event loop avoiding starvation.
            async for msg in self.__map_stream_handler(list(req.request.keys), datum):
                res = map_pb2.MapResponse.Result(keys=msg.keys, value=msg.value, tags=msg.tags)
                await result_queue.put(map_pb2.MapResponse(results=[res], id=req.id))
                await asyncio.sleep(0)

            # Emit EOT for this request id
            await result_queue.put(
                map_pb2.MapResponse(status=map_pb2.TransmissionStatus(eot=True), id=req.id)
            )

        except BaseException as err:
            _LOGGER.critical("MapFn handler error", exc_info=True)
            # Surface handler error to the main producer;
            # it will set the shutdown event and end the RPC
            await result_queue.put(err)

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> map_pb2.ReadyResponse:
        """Heartbeat endpoint for gRPC."""
        return map_pb2.ReadyResponse(ready=True)
