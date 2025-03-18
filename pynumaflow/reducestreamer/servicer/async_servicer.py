import asyncio
from collections.abc import AsyncIterable
from typing import Union

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow._constants import ERR_UDF_EXCEPTION_STRING
from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from pynumaflow.reducestreamer._dtypes import (
    Datum,
    ReduceStreamAsyncCallable,
    _ReduceStreamBuilderClass,
    ReduceRequest,
)
from pynumaflow.reducestreamer.servicer.task_manager import TaskManager
from pynumaflow.shared.server import handle_async_error
from pynumaflow.types import NumaflowServicerContext


async def datum_generator(
    request_iterator: AsyncIterable[reduce_pb2.ReduceRequest],
) -> AsyncIterable[ReduceRequest]:
    """Generate a ReduceRequest from a ReduceRequest proto message."""
    async for d in request_iterator:
        reduce_request = ReduceRequest(
            operation=d.operation.event,
            windows=d.operation.windows,
            payload=Datum(
                keys=list(d.payload.keys),
                value=d.payload.value,
                event_time=d.payload.event_time.ToDatetime(),
                watermark=d.payload.watermark.ToDatetime(),
                headers=dict(d.payload.headers),
            ),
        )
        yield reduce_request


class AsyncReduceStreamServicer(reduce_pb2_grpc.ReduceServicer):
    """
    This class is used to create a new grpc Reduce servicer instance.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: Union[ReduceStreamAsyncCallable, _ReduceStreamBuilderClass],
    ):
        # The Reduce handler can be a function or a builder class instance.
        self.__reduce_handler: Union[ReduceStreamAsyncCallable, _ReduceStreamBuilderClass] = handler

    async def ReduceFn(
        self,
        request_iterator: AsyncIterable[reduce_pb2.ReduceRequest],
        context: NumaflowServicerContext,
    ) -> reduce_pb2.ReduceResponse:
        """
        Applies a reduce function to a datum stream.
        The pascal case function name comes from the proto reduce_pb2_grpc.py file.
        """
        # Create a task manager instance
        task_manager = TaskManager(handler=self.__reduce_handler)

        # Create a consumer task to read from the result queue
        # All the results from the reduce function will be sent to the result queue
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
        # 1. A reduce_pb2.ReduceResponse message
        # This is the result of the reduce function, it contains the window and the
        # result of the reduce function
        # The result of the reduce function is a reduce_pb2.ReduceResponse message and can be
        # directly sent to the client
        #
        # 2. An Exception
        # Any exceptions that occur during the processing reduce function tasks are
        # sent to the result queue. We then forward these exception to the client
        #
        # 3. A reduce_pb2.ReduceResponse message with EOF=True
        # This is a special message that indicates the end of the processing for a window
        # When we get this message, we send an EOF message to the client
        try:
            async for msg in consumer:
                # If the message is an exception, we raise the exception
                if isinstance(msg, BaseException):
                    await handle_async_error(context, msg, ERR_UDF_EXCEPTION_STRING)
                    return
                # Send window EOF response or Window result response
                # back to the client
                else:
                    yield msg
        except BaseException as e:
            await handle_async_error(context, e, ERR_UDF_EXCEPTION_STRING)
            return
        # Wait for the process_input_stream task to finish for a clean exit
        try:
            await producer
        except BaseException as e:
            await handle_async_error(context, e, ERR_UDF_EXCEPTION_STRING)
            return

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> reduce_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto reduce_pb2_grpc.py file.
        """
        return reduce_pb2.ReadyResponse(ready=True)
