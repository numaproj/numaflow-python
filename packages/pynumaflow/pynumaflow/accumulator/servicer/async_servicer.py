import asyncio
from collections.abc import AsyncIterable

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow._constants import _LOGGER, ERR_UDF_EXCEPTION_STRING
from pynumaflow.proto.accumulator import accumulator_pb2, accumulator_pb2_grpc
from pynumaflow.accumulator._dtypes import (
    Datum,
    AccumulatorAsyncCallable,
    _AccumulatorBuilderClass,
    AccumulatorRequest,
    KeyedWindow,
)
from pynumaflow.accumulator.servicer.task_manager import TaskManager
from pynumaflow.shared.server import update_context_err
from pynumaflow.types import NumaflowServicerContext


async def datum_generator(
    request_iterator: AsyncIterable[accumulator_pb2.AccumulatorRequest],
) -> AsyncIterable[AccumulatorRequest]:
    """Generate a AccumulatorRequest from a AccumulatorRequest proto message."""
    async for d in request_iterator:
        # Convert protobuf KeyedWindow to our KeyedWindow dataclass
        keyed_window = KeyedWindow(
            start=d.operation.keyedWindow.start.ToDatetime(),
            end=d.operation.keyedWindow.end.ToDatetime(),
            slot=d.operation.keyedWindow.slot,
            keys=list(d.operation.keyedWindow.keys),
        )

        accumulator_request = AccumulatorRequest(
            operation=d.operation.event,
            keyed_window=keyed_window,  # Use the new parameter name
            payload=Datum(
                keys=list(d.payload.keys),
                value=d.payload.value,
                event_time=d.payload.event_time.ToDatetime(),
                watermark=d.payload.watermark.ToDatetime(),
                id_=d.payload.id,
                headers=dict(d.payload.headers),
            ),
        )
        yield accumulator_request


class AsyncAccumulatorServicer(accumulator_pb2_grpc.AccumulatorServicer):
    """
    This class is used to create a new grpc Accumulator servicer instance.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: AccumulatorAsyncCallable | _AccumulatorBuilderClass,
    ):
        # The accumulator handler can be a function or a builder class instance.
        self.__accumulator_handler: AccumulatorAsyncCallable | _AccumulatorBuilderClass = handler
        self._shutdown_event: asyncio.Event | None = None
        self._error: BaseException | None = None

    def set_shutdown_event(self, event: asyncio.Event):
        """Wire up the shutdown event created by the server's aexec() coroutine."""
        self._shutdown_event = event

    async def AccumulateFn(
        self,
        request_iterator: AsyncIterable[accumulator_pb2.AccumulatorRequest],
        context: NumaflowServicerContext,
    ) -> accumulator_pb2.AccumulatorResponse:
        """
        Applies a accumulator function to a datum stream.
        The pascal case function name comes from the proto accumulator_pb2_grpc.py file.
        """
        # Create a task manager instance
        task_manager = TaskManager(handler=self.__accumulator_handler)

        # Create a consumer task to read from the result queue
        # All the results from the accumulator function will be sent to the result queue
        # We will read from the result queue and send the results to the client
        consumer = task_manager.global_result_queue.read_iterator()

        # Create an async iterator from the request iterator
        datum_iterator = datum_generator(request_iterator=request_iterator)

        # Create a process_input_stream task in the task manager,
        # this would read from the datum iterator
        # and then create the required tasks to process the data requests
        # The results from these tasks are then sent to the result queue
        producer = asyncio.create_task(task_manager.process_input_stream(datum_iterator))

        # Start the consumer task where we read from the result queue
        # and send the results to the client
        # The task manager can write the following to the result queue:
        # 1. A accumulator_pb2.AccumulatorResponse message
        # This is the result of the accumulator function, it contains the window and the
        # result of the accumulator function
        # The result of the accumulator function is a accumulator_pb2.AccumulatorResponse message
        #  and can be directly sent to the client
        #
        # 2. An Exception
        # Any exceptions that occur during the processing accumulator function tasks are
        # sent to the result queue. We then forward these exception to the client
        #
        # 3. A accumulator_pb2.AccumulatorResponse message with EOF=True
        # This is a special message that indicates the end of the processing for a window
        # When we get this message, we send an EOF message to the client
        try:
            async for msg in consumer:
                # If the message is an exception, we raise the exception
                if isinstance(msg, BaseException):
                    err_msg = f"{ERR_UDF_EXCEPTION_STRING}: {repr(msg)}"
                    _LOGGER.critical(err_msg, exc_info=True)
                    update_context_err(context, msg, err_msg)
                    self._error = msg
                    if self._shutdown_event is not None:
                        self._shutdown_event.set()
                    return
                # Send window EOF response or Window result response
                # back to the client
                else:
                    yield msg
        except BaseException as e:
            err_msg = f"{ERR_UDF_EXCEPTION_STRING}: {repr(e)}"
            _LOGGER.critical(err_msg, exc_info=True)
            update_context_err(context, e, err_msg)
            self._error = e
            if self._shutdown_event is not None:
                self._shutdown_event.set()
            return
        # Wait for the process_input_stream task to finish for a clean exit
        try:
            await producer
        except BaseException as e:
            err_msg = f"{ERR_UDF_EXCEPTION_STRING}: {repr(e)}"
            _LOGGER.critical(err_msg, exc_info=True)
            update_context_err(context, e, err_msg)
            self._error = e
            if self._shutdown_event is not None:
                self._shutdown_event.set()
            return

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> accumulator_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto accumulator_pb2_grpc.py file.
        """
        return accumulator_pb2.ReadyResponse(ready=True)
