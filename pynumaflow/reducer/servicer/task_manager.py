import asyncio
from datetime import datetime, timezone
from typing import Union
from collections.abc import AsyncIterable

from pynumaflow.exceptions import UDFError
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
    ReduceWindow,
)


def get_unique_key(keys, window):
    return f"{window.start.ToMilliseconds()}:{window.end.ToMilliseconds()}:{DELIMITER.join(keys)}"


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
            resps.append(reduce_pb2.ReduceResponse(window=window.to_proto(), eof=True))
        return resps

    async def stream_send_eof(self):
        for unified_key in self.tasks:
            await self.tasks[unified_key].iterator.put(STREAM_EOF)

    async def create_task(self, req):
        # if len of windows in request != 1, raise error
        if len(req.windows) != 1:
            raise UDFError("reduce create operation error: invalid number of windows")

        d = req.payload
        keys = d.keys()
        unified_key = get_unique_key(keys, req.windows[0])
        result = self.tasks.get(unified_key, None)

        if not result:
            niter = NonBlockingIterator()
            riter = niter.read_iterator()
            task = asyncio.create_task(self.__invoke_reduce(keys, riter, req.windows[0]))
            # Save a reference to the result of this function, to avoid a
            # task disappearing mid-execution.
            self.background_tasks.add(task)
            task.add_done_callback(lambda t: self.background_tasks.remove(t))
            result = ReduceResult(task, niter, keys, req.windows[0])

            # Save the result of the reduce operation to the task list
            self.tasks[unified_key] = result

        # Put the request in the iterator
        await result.iterator.put(d)

    async def append_task(self, req):
        if len(req.windows) != 1:
            raise UDFError("reduce create operation error: invalid number of windows")
        d = req.payload
        keys = d.keys()
        unified_key = get_unique_key(keys, req.windows[0])
        result = self.tasks.get(unified_key, None)
        if not result:
            await self.create_task(req)
        else:
            await result.iterator.put(d)

    async def __invoke_reduce(
        self, keys: list[str], request_iterator: AsyncIterable[Datum], window: ReduceWindow
    ):
        new_instance = self.__reduce_handler
        start_dt = datetime.fromtimestamp(int(window.start.ToMilliseconds()) / 1e3, timezone.utc)
        end_dt = datetime.fromtimestamp(int(window.end.ToMilliseconds()) / 1e3, timezone.utc)
        interval_window = IntervalWindow(start_dt, end_dt)
        md = Metadata(interval_window=interval_window)
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
