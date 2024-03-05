import asyncio
from typing import AsyncIterable, Union
from pynumaflow.proto.reducer import reduce_pb2
from pynumaflow.reducer.servicer.asynciter import NonBlockingIterator
from pynumaflow._constants import (
    STREAM_EOF,
    DELIMITER,
    _LOGGER,
)
from pynumaflow.reducer._dtypes import (
    IntervalWindow,
    Metadata,
    ReduceResult,
    Datum,
    _ReduceBuilderClass,
    ReduceAsyncCallable,
)


def get_unique_key(keys, window):
    return f"{window.start}:{window.end}:{DELIMITER.join(keys)}"


class TaskManager:
    def __init__(self, handler: Union[ReduceAsyncCallable, _ReduceBuilderClass]):
        self.tasks = {}
        self.background_tasks = set()
        self.__reduce_handler = handler

    def get_tasks(self):
        return self.tasks.values()

    def build_eof_responses(self):
        window_set = set()
        # Extract the windows from the tasks
        for unified_key in self.tasks.keys():
            window_set.add(self.tasks[unified_key].window)
        # Build the response
        resps = []
        for window in window_set:
            resps.append(
                reduce_pb2.ReduceResponse(window=window.to_proto(), eof=True)
                )
        return resps
    
    async def stream_send_eof(self):
        for unified_key in self.tasks.keys():
            await self.tasks[unified_key].iterator.put(STREAM_EOF)

    async def create_task(self, req):
        # if len of windows in request != 1, raise error
        d = req.payload
        keys = d.keys()
        unified_key = get_unique_key(keys, req.windows[0])
        result = self.tasks.get(unified_key, None)
        interval_window = IntervalWindow(req.windows[0].start, req.windows[0].end)

        if not result:
            niter = NonBlockingIterator()
            riter = niter.read_iterator()
            task = asyncio.create_task(
                self.__invoke_reduce(keys, riter, Metadata(interval_window=interval_window))
            )
            # Save a reference to the result of this function, to avoid a
            # task disappearing mid-execution.
            self.background_tasks.add(task)
            task.add_done_callback(lambda t: self.background_tasks.remove(t))
            result = ReduceResult(task, niter, keys, req.windows[0])

            # Save the result of the reduce operation to the task list
            self.tasks[unified_key] = result

        # Put the request in the iterator
        await result.iterator.put(d)

    async def append_task(self, request):
        d = request.payload
        keys = d.keys()
        unified_key = get_unique_key(keys, request.windows[0])
        result = self.tasks.get(unified_key, None)
        if not result:
            await self.create_task(request)
        await result.iterator.put(d)
    

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
