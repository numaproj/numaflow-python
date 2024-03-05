from collections.abc import AsyncIterable
from typing import Union

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow._constants import _LOGGER
from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from pynumaflow.reducer._dtypes import (
    Datum,
    ReduceAsyncCallable,
    _ReduceBuilderClass,
    ReduceRequest,
    WindowOperation,
)
from pynumaflow.reducer.servicer.task_manager import TaskManager
from pynumaflow.types import NumaflowServicerContext


async def datum_generator(
    request_iterator: AsyncIterable[reduce_pb2.ReduceRequest],
) -> AsyncIterable[ReduceRequest]:
    async for d in request_iterator:
        reduce_request = ReduceRequest(
            operation=d.operation.event,
            windows=d.operation.windows,
            payload=Datum(
                keys=list(d.payload.keys),
                value=d.payload.value,
                event_time=d.payload.event_time.ToDatetime(),
                watermark=d.payload.watermark.ToDatetime(),
            ),
        )
        yield reduce_request


class AsyncReduceServicer(reduce_pb2_grpc.ReduceServicer):
    """
    This class is used to create a new grpc Reduce servicer instance.
    It implements the SyncMapServicer interface from the proto reduce.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: Union[ReduceAsyncCallable, _ReduceBuilderClass],
    ):
        # The Reduce handler can be a function or a builder class instance.
        self.__reduce_handler: Union[ReduceAsyncCallable, _ReduceBuilderClass] = handler

    async def ReduceFn(
        self,
        request_iterator: AsyncIterable[reduce_pb2.ReduceRequest],
        context: NumaflowServicerContext,
    ) -> reduce_pb2.ReduceResponse:
        """
        Applies a reduce function to a datum stream.
        The pascal case function name comes from the proto reduce_pb2_grpc.py file.
        """
        # Create an async iterator from the request iterator
        datum_iterator = datum_generator(request_iterator=request_iterator)

        # Create a task manager instance
        task_manager = TaskManager(handler=self.__reduce_handler)

        # Start iterating through the request iterator and create tasks
        # based on the operation type received.
        try:
            async for request in datum_iterator:
                # check whether the request is an open or append operation
                if request.operation is int(WindowOperation.OPEN):
                    # create a new task for the open operation
                    await task_manager.create_task(request)
                elif request.operation is int(WindowOperation.APPEND):
                    # append the task data to the existing task
                    await task_manager.append_task(request)
        except Exception as e:
            _LOGGER.critical("Reduce Error", exc_info=True)
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(e.__str__())
            yield reduce_pb2.ReduceResponse()
            raise e

        # send EOF to all the tasks once the request iterator is exhausted
        # This will signal the tasks to stop reading the data on their
        # respective iterators.
        await task_manager.stream_send_eof()

        # get the results from all the tasks
        res = task_manager.get_tasks()
        try:
            # iterate through the tasks and yield the response
            # once the task is completed.
            for task in res:
                fut = task.future
                await fut
                for msg in fut.result():
                    yield reduce_pb2.ReduceResponse(result=msg, window=task.window)
                yield reduce_pb2.ReduceResponse(window=task.window, EOF=True)
        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(e.__str__())
            yield reduce_pb2.ReduceResponse()
            raise e

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> reduce_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto reduce_pb2_grpc.py file.
        """
        return reduce_pb2.ReadyResponse(ready=True)
