import asyncio
from datetime import datetime, timezone
from typing import Union
from collections.abc import AsyncIterable

import grpc

from pynumaflow.exceptions import UDFError
from pynumaflow.proto.reducer import reduce_pb2
from pynumaflow.shared.asynciter import NonBlockingIterator
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
from pynumaflow.shared.server import exit_on_error
from pynumaflow.types import NumaflowServicerContext


def build_unique_key_name(keys, window):
    """
    Builds a unique key name for the given keys and window.
    The key name is used to identify the Reduce task.
    The format is: start_time:end_time:key1:key2:...
    """
    return f"{window.start.ToMilliseconds()}:{window.end.ToMilliseconds()}:{DELIMITER.join(keys)}"


def build_window_hash(window):
    """
    Builds a hash for the given window.
    The hash is used to identify the Reduce Window
    The format is: start_time:end_time
    """
    return f"{window.start.ToMilliseconds()}:{window.end.ToMilliseconds()}"


class TaskManager:
    """
    TaskManager is responsible for managing the Reduce tasks.
    It is created whenever a new reduce operation is requested.
    """

    def __init__(
        self,
        handler: Union[ReduceAsyncCallable, _ReduceBuilderClass],
        context: NumaflowServicerContext,
    ):
        # A dictionary to store the task information
        self.tasks = {}
        # Collection for storing strong references to all running tasks.
        # Event loop only keeps a weak reference, which can cause it to
        # get lost during execution.
        self.background_tasks = set()
        # Handler for the reduce operation
        self.__reduce_handler = handler
        # Servicer context from grpc, required to abort if error occurs
        self.context = context

    def get_tasks(self):
        """
        Returns the list of reduce tasks that are
        currently being processed
        """
        return self.tasks.values()

    def get_unique_windows(self):
        """
        Returns the unique windows that are currently being processed
        """
        # Dict to store unique windows
        windows = dict()
        # Iterate over all the tasks and add the windows
        for task in self.tasks.values():
            window_hash = build_window_hash(task.window)
            window_found = windows.get(window_hash, None)
            # if window not seen yet, add to the dict
            if not window_found:
                windows[window_hash] = task.window
        return windows

    async def stream_send_eof(self):
        """
        Sends EOF to input streams of all the Reduce
        tasks that are currently being processed.
        This is called when the input grpc stream is closed.
        """
        for unified_key in self.tasks:
            await self.tasks[unified_key].iterator.put(STREAM_EOF)

    async def create_task(self, req):
        """
        Creates a new reduce task for the given request.
        Based on the request we compute a unique key, and then
        it creates a new task or appends the request to the existing task.
        """
        # if len of windows in request != 1, raise error
        if len(req.windows) != 1:
            raise UDFError("reduce create operation error: invalid number of windows")

        d = req.payload
        keys = d.keys()
        unified_key = build_unique_key_name(keys, req.windows[0])
        result = self.tasks.get(unified_key, None)

        # If the task does not exist, create a new task
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
        """
        Appends the request to the existing window reduce task.
        If the task does not exist, create it.
        """
        if len(req.windows) != 1:
            raise UDFError("reduce create operation error: invalid number of windows")
        d = req.payload
        keys = d.keys()
        unified_key = build_unique_key_name(keys, req.windows[0])
        result = self.tasks.get(unified_key, None)
        if not result:
            await self.create_task(req)
        else:
            await result.iterator.put(d)

    async def __invoke_reduce(
        self, keys: list[str], request_iterator: AsyncIterable[Datum], window: ReduceWindow
    ):
        """
        Invokes the UDF reduce handler with the given keys,
        request iterator, and window. Returns the result of the
        reduce operation.
        """
        new_instance = self.__reduce_handler

        # Convert the window to a datetime object
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
        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            # Send a context abort signal for the rpc, this is required for numa container to get
            # the correct grpc error
            await asyncio.gather(
                self.context.abort(grpc.StatusCode.UNKNOWN, details=repr(err)),
                return_exceptions=True,
            )
            exit_on_error(err=repr(err), parent=False, context=self.context, update_context=False)
            return

        datum_responses = []
        for msg in msgs:
            datum_responses.append(
                reduce_pb2.ReduceResponse.Result(keys=msg.keys, value=msg.value, tags=msg.tags)
            )

        return datum_responses
