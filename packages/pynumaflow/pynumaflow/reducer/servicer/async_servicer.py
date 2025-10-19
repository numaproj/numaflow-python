from collections.abc import AsyncIterable
from typing import Union

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow._constants import _LOGGER, ERR_UDF_EXCEPTION_STRING
from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from pynumaflow.reducer._dtypes import (
    Datum,
    ReduceAsyncCallable,
    _ReduceBuilderClass,
    ReduceRequest,
    WindowOperation,
)
from pynumaflow.reducer.servicer.task_manager import TaskManager
from pynumaflow.shared.server import handle_async_error
from pynumaflow.types import NumaflowServicerContext


async def datum_generator(
    request_iterator: AsyncIterable[reduce_pb2.ReduceRequest],
) -> AsyncIterable[ReduceRequest]:
    """
    This function is used to create an async generator
    from the gRPC request iterator.
    It yields a ReduceRequest instance for each request received which is then
    forwarded to the task manager.
    """
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

        The lifecycle of the reduce operation is managed by the task manager. Which is created
        for each new grpc call.

        Whenever a new request is received, the task manager invokes the required
        operation based on the request type.
        1) If the request is for a new keyed window, a new task is created.
           If the request is for an existing keyed window, the data is
           appended to the task.

        2) Once the request iterator is exhausted, the task manager sends EOF to all
        the tasks, to signal them to stop reading the data from their respective iterators.

        3) The task manager then waits for the tasks to complete and
        yields the response from each task

        """
        # Create an async iterator from the request iterator
        datum_iterator = datum_generator(request_iterator=request_iterator)

        # Create a task manager instance
        # The task manager is used to manage lifecycle of the tasks
        # required for the reduce operation.
        task_manager = TaskManager(handler=self.__reduce_handler, context=context)

        # Start iterating through the request iterator and create tasks
        # based on the operation type received.
        try:
            async for request in datum_iterator:
                # check whether the request is an open or append operation
                if request.operation is int(WindowOperation.OPEN):
                    # create a new task for the open operation
                    # and inserts the request data into the task
                    await task_manager.create_task(request)
                elif request.operation is int(WindowOperation.APPEND):
                    # append the task data to the existing task
                    # if the task does not exist, it will create a new task
                    await task_manager.append_task(request)
        except BaseException as e:
            _LOGGER.critical("Reduce Error", exc_info=True)
            # Send a context abort signal for the rpc, this is required for numa container to get
            # the correct grpc error
            await handle_async_error(context, e, ERR_UDF_EXCEPTION_STRING)

        # send EOF to all the tasks once the request iterator is exhausted
        # This will signal the tasks to stop reading the data on their
        # respective iterators.
        await task_manager.stream_send_eof()

        # Get the list of tasks from the task manager
        res = task_manager.get_tasks()
        try:
            # iterate through the tasks and yield the response
            # from each of them once the task is completed.
            for task in res:
                fut = task.future
                await fut

                # For each message in the task result, yield the response
                if fut.result():
                    for msg in fut.result():
                        yield reduce_pb2.ReduceResponse(result=msg, window=task.window)

            # For each window processed by the ReduceFn send an EOF response
            # We send one EOF per window
            current_window = task_manager.get_unique_windows()
            for window in current_window.values():
                # yield the EOF response once the task is completed for a keyed window
                yield reduce_pb2.ReduceResponse(window=window, EOF=True)
        except BaseException as e:
            _LOGGER.critical("Reduce Error", exc_info=True)
            # Send a context abort signal for the rpc, this is required for numa container to get
            # the correct grpc error
            await handle_async_error(context, e, ERR_UDF_EXCEPTION_STRING)

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> reduce_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto reduce_pb2_grpc.py file.
        """
        return reduce_pb2.ReadyResponse(ready=True)
