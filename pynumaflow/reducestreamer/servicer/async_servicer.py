import asyncio
from collections.abc import AsyncIterable
from typing import Union

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from pynumaflow.reducestreamer._dtypes import (
    Datum,
    ReduceStreamAsyncCallable,
    _ReduceStreamBuilderClass,
    ReduceRequest,
)
from pynumaflow.reducestreamer.servicer.task_manager import TaskManager
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
        # Create an async iterator from the request iterator
        datum_iterator = datum_generator(request_iterator=request_iterator)

        # Create a task manager instance
        task_manager = TaskManager(handler=self.__reduce_handler)

        consumer = task_manager.result_queue.read_iterator()
        producer = asyncio.create_task(task_manager.producer(datum_iterator))

        try:
            async for msg in consumer:
                if isinstance(msg, reduce_pb2.Window):
                    yield reduce_pb2.ReduceResponse(window=msg, EOF=True)
                else:
                    yield msg
                    # res = reduce_pb2.ReduceResponse.Result(
                    #     keys=msg.keys, value=msg.value, tags=msg.tags
                    # )
                    # yield reduce_pb2.ReduceResponse(result=res, window=msg.window)
        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(e.__str__())
            yield reduce_pb2.ReduceResponse()
            raise e

        try:
            await producer
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
