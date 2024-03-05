import asyncio

from datetime import datetime, timezone
from collections.abc import AsyncIterable
from typing import Union

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow._constants import (
    WIN_START_TIME,
    WIN_END_TIME,
    STREAM_EOF,
    DELIMITER,
)
from pynumaflow.reducer._dtypes import (
    Datum,
    IntervalWindow,
    Metadata,
    ReduceAsyncCallable,
    _ReduceBuilderClass, ReduceRequest, WindowOperation,
)
from pynumaflow.reducer._dtypes import ReduceResult
from pynumaflow.reducer.servicer.asynciter import NonBlockingIterator
from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER


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
            )

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
        # Collection for storing strong references to all running tasks.
        # Event loop only keeps a weak reference, which can cause it to
        # get lost during execution.
        self.background_tasks = set()
        # The reduce handler can be a function or a builder class instance.
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
        _LOGGER.info("ENTERING REQUEST")

        # start, end = None, None
        # for metadata_key, metadata_value in context.invocation_metadata():
        #     if metadata_key == WIN_START_TIME:
        #         start = metadata_value
        #     elif metadata_key == WIN_END_TIME:
        #         end = metadata_value
        # if not (start or end):
        #     context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        #     context.set_details(
        #         f"Expected to have all key/window_start_time/window_end_time; "
        #         f"got start: {start}, end: {end}."
        #     )
        #     yield reduce_pb2.ReduceResponse(results=[])
        #     return
        #
        # start_dt = datetime.fromtimestamp(int(start) / 1e3, timezone.utc)
        # end_dt = datetime.fromtimestamp(int(end) / 1e3, timezone.utc)
        # interval_window = IntervalWindow(start=start_dt, end=end_dt)

        datum_iterator = datum_generator(request_iterator=request_iterator)

        response_task = asyncio.create_task(
            self.__async_reduce_handler(datum_iterator)
        )

        # Save a reference to the result of this function, to avoid a
        # task disappearing mid-execution.
        self.background_tasks.add(response_task)
        response_task.add_done_callback(lambda t: self.background_tasks.remove(t))

        await response_task
        results_futures = response_task.result()
        print("RES", type(results_futures[0][1]))

        try:
            for tup in results_futures:
                fut, window = tup[0], tup[1]
                print("FIT", type(fut), "WIND", type(window))
                await fut
                yield reduce_pb2.ReduceResponse(result=fut.result()[0], window=window)
        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(e.__str__())
            yield reduce_pb2.ReduceResponse(result=[], window=None, EOF=True)

    async def __async_reduce_handler(self, datum_iterator: AsyncIterable[ReduceRequest]):
        callable_dict = {}
        async for req in datum_iterator:
            if req.operation is int(WindowOperation.OPEN):
                _LOGGER.info("OPEN")
                d = req.payload
                keys = d.keys()
                unified_key = DELIMITER.join(keys)
                result = callable_dict.get(unified_key, None)
                interval_window = IntervalWindow(req.windows[0].start, req.windows[0].end)

                if not result:
                    niter = NonBlockingIterator()
                    riter = niter.read_iterator()
                    # schedule an async task for consumer
                    # returns a future that will give the results later.
                    task = asyncio.create_task(
                        self.__invoke_reduce(keys, riter, Metadata(interval_window=interval_window))
                    )
                    # Save a reference to the result of this function, to avoid a
                    # task disappearing mid-execution.
                    self.background_tasks.add(task)
                    task.add_done_callback(lambda t: self.background_tasks.remove(t))
                    result = ReduceResult(task, niter, keys, req.windows[0])

                    callable_dict[unified_key] = result

                await result.iterator.put(d)
            elif req.operation is int(WindowOperation.APPEND):
                _LOGGER.info("Append")
                d = req.payload
                keys = d.keys()
                unified_key = DELIMITER.join(keys)
                result = callable_dict.get(unified_key, None)
                interval_window = IntervalWindow(req.windows[0].start, req.windows[0].end)

                if not result:
                    niter = NonBlockingIterator()
                    riter = niter.read_iterator()
                    # schedule an async task for consumer
                    # returns a future that will give the results later.
                    task = asyncio.create_task(
                        self.__invoke_reduce(keys, riter, Metadata(interval_window=interval_window))
                    )
                    # Save a reference to the result of this function, to avoid a
                    # task disappearing mid-execution.
                    self.background_tasks.add(task)
                    task.add_done_callback(lambda t: self.background_tasks.remove(t))
                    result = ReduceResult(task, niter, keys, req.windows[0])

                    callable_dict[unified_key] = result

                await result.iterator.put(d)
        # # iterate through all the values
        # async for d in datum_iterator:
        #     keys = d.keys()
        #     unified_key = DELIMITER.join(keys)
        #     result = callable_dict.get(unified_key, None)
        #
        #     if not result:
        #         niter = NonBlockingIterator()
        #         riter = niter.read_iterator()
        #         # schedule an async task for consumer
        #         # returns a future that will give the results later.
        #         task = asyncio.create_task(
        #             self.__invoke_reduce(keys, riter, Metadata(interval_window=interval_window))
        #         )
        #         # Save a reference to the result of this function, to avoid a
        #         # task disappearing mid-execution.
        #         self.background_tasks.add(task)
        #         task.add_done_callback(lambda t: self.background_tasks.remove(t))
        #         result = ReduceResult(task, niter, keys)
        #
        #         callable_dict[unified_key] = result
        #
        #     await result.iterator.put(d)

        for unified_key in callable_dict:
            await callable_dict[unified_key].iterator.put(STREAM_EOF)

        tasks = []
        for unified_key in callable_dict:
            fut = callable_dict[unified_key].future
            tasks.append((fut, callable_dict[unified_key].window))

        return tasks

    async def __invoke_reduce(
            self, keys: list[str], request_iterator: AsyncIterable[Datum], md: Metadata
    ):
        new_instance = self.__reduce_handler
        # If the reduce handler is a class instance, create a new instance of it.
        # It is required for a new key to be processed by a
        # new instance of the reducer for a given window
        # Otherwise the function handler can be called directly
        if isinstance(self.__reduce_handler, _ReduceBuilderClass):
            new_instance = self.__reduce_handler.create()
        try:
            msgs = await new_instance(keys, request_iterator, md)
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            raise err

        datum_responses = []
        for msg in msgs:
            datum_responses.append(
                reduce_pb2.ReduceResponse.Result(keys=msg.keys, value=msg.value, tags=msg.tags)
            )

        return datum_responses

    async def IsReady(
            self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> reduce_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto reduce_pb2_grpc.py file.
        """
        return reduce_pb2.ReadyResponse(ready=True)
