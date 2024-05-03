import asyncio
from datetime import datetime, timezone
from typing import Union
from collections.abc import AsyncIterable

from pynumaflow.exceptions import UDFError
from pynumaflow.proto.reducer import reduce_pb2
from pynumaflow.shared.asynciter import NonBlockingIterator
from pynumaflow._constants import (
    STREAM_EOF,
    DELIMITER,
    _LOGGER,
)
from pynumaflow.reducestreamer._dtypes import (
    IntervalWindow,
    ReduceResult,
    Datum,
    _ReduceStreamBuilderClass,
    ReduceStreamAsyncCallable,
    ReduceWindow,
    WindowOperation,
    Metadata,
)


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


def create_window_eof_response(window):
    """Create a Reduce response with EOF=True for a given window"""
    return reduce_pb2.ReduceResponse(window=window, EOF=True)


class TaskManager:
    """
    TaskManager is responsible for managing the Reduce tasks.
    It is created whenever a new reduce operation is requested.
    """

    def __init__(self, handler: Union[ReduceStreamAsyncCallable, _ReduceStreamBuilderClass]):
        # A dictionary to store the task information
        self.tasks = {}
        # Collection for storing strong references to all running tasks.
        # Event loop only keeps a weak reference, which can cause it to
        # get lost during execution.
        self.background_tasks = set()
        # Handler for the reduce operation
        self.__reduce_handler = handler
        # Queue to store the results of the reduce operation
        # This queue is used to send the results to the client
        # once the reduce operation is completed.
        # This queue is also used to send the error/exceptions to the client
        # if the reduce operation fails.
        self.global_result_queue = NonBlockingIterator()

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

    def get_tasks(self):
        """
        Returns the list of reduce tasks that are
        currently being processed
        """
        return list(self.tasks.values())

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
        curr_task = self.tasks.get(unified_key, None)

        # If the task does not exist, create a new task
        if not curr_task:
            niter = NonBlockingIterator()
            riter = niter.read_iterator()
            # Create a new result queue for the current task
            # We create a new result queue for each task, so that
            # the results of the reduce operation can be sent to the
            # the global result queue, which in turn sends the results
            # to the client.
            res_queue = NonBlockingIterator()

            # Create a new write_to_global_queue task for the current, this will read from the
            # result queue specifically for the current task and update the
            # global result queue
            consumer = asyncio.create_task(
                self.write_to_global_queue(res_queue, self.global_result_queue, req.windows[0])
            )
            # Save a reference to the result of this function, to avoid a
            # task disappearing mid-execution.
            self.background_tasks.add(consumer)
            consumer.add_done_callback(self.clean_background)

            # Create a new task for the reduce operation, this will invoke the
            # Reduce handler with the given keys, request iterator, and window.
            task = asyncio.create_task(self.__invoke_reduce(keys, riter, res_queue, req.windows[0]))
            # Save a reference to the result of this function, to avoid a
            # task disappearing mid-execution.
            self.background_tasks.add(task)
            task.add_done_callback(self.clean_background)

            # Create a new ReduceResult object to store the task information
            curr_task = ReduceResult(task, niter, keys, req.windows[0], res_queue, consumer)

            # Save the result of the reduce operation to the task list
            self.tasks[unified_key] = curr_task

        # Put the request in the iterator
        await curr_task.iterator.put(d)

    async def send_datum_to_task(self, req):
        """
        Appends the request to the existing window reduce task.
        If the task does not exist, create it.
        """
        if len(req.windows) != 1:
            raise UDFError("reduce append operation error: invalid number of windows")
        d = req.payload
        keys = d.keys()
        unified_key = build_unique_key_name(keys, req.windows[0])
        result = self.tasks.get(unified_key, None)
        if not result:
            await self.create_task(req)
        else:
            await result.iterator.put(d)

    async def __invoke_reduce(
        self,
        keys: list[str],
        request_iterator: AsyncIterable[Datum],
        output: NonBlockingIterator,
        window: ReduceWindow,
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
        if isinstance(self.__reduce_handler, _ReduceStreamBuilderClass):
            new_instance = self.__reduce_handler.create()
        try:
            _ = await new_instance(keys, request_iterator, output, md)
        # If there is an error in the reduce operation, log and
        # then send the error to the result queue
        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            # Put the exception in the result queue
            await self.global_result_queue.put(err)

    async def process_input_stream(self, request_iterator: AsyncIterable[reduce_pb2.ReduceRequest]):
        # Start iterating through the request iterator and create tasks
        # based on the operation type received.
        try:
            async for request in request_iterator:
                # check whether the request is an open or append operation
                if request.operation is int(WindowOperation.OPEN):
                    # create a new task for the open operation and
                    # put the request in the task iterator
                    await self.create_task(request)
                elif request.operation is int(WindowOperation.APPEND):
                    # append the task data to the existing task
                    # if the task does not exist, create a new task
                    await self.send_datum_to_task(request)
        # If there is an error in the reduce operation, log and
        # then send the error to the result queue
        except BaseException as e:
            err_msg = f"Reduce Streaming Error: {repr(e)}"
            _LOGGER.critical(err_msg, exc_info=True)
            # Put the exception in the global result queue
            await self.global_result_queue.put(e)
            return

        try:
            # send EOF to all the tasks once the request iterator is exhausted
            # This will signal the tasks to stop reading the data on their
            # respective iterators.
            await self.stream_send_eof()

            # get the list of reduce tasks that are currently being processed
            # iterate through the tasks and wait for them to complete
            for task in self.get_tasks():
                # Once this is done, we know that the task has written all the results
                # to the local result queue
                fut = task.future
                await fut

                # Send an EOF message to the local result queue
                # This will signal that the task has completed processing
                await task.result_queue.put(STREAM_EOF)

                # Wait for the local queue to write
                # all the results of this task to the global result queue
                con_future = task.consumer_future
                await con_future

            # Once all tasks are completed, senf EOF to all windows that
            # were processed in the Task Manager. We send a single
            # EOF message per window.
            current_windows = self.get_unique_windows()
            for window in current_windows.values():
                # Send an EOF message to the global result queue
                # This will signal that window has been processed
                eof_window_msg = create_window_eof_response(window=window)
                await self.global_result_queue.put(eof_window_msg)

            # Once all tasks are completed, senf EOF the global result queue
            await self.global_result_queue.put(STREAM_EOF)
        except BaseException as e:
            err_msg = f"Reduce Streaming Error: {repr(e)}"
            _LOGGER.critical(err_msg, exc_info=True)
            await self.global_result_queue.put(e)

    async def write_to_global_queue(
        self, input_queue: NonBlockingIterator, output_queue: NonBlockingIterator, window
    ):
        """
        This task is for given Reduce task.
        This would from the local result queue for the task and then write
        to the global result queue
        """
        reader = input_queue.read_iterator()
        async for msg in reader:
            res = reduce_pb2.ReduceResponse.Result(keys=msg.keys, value=msg.value, tags=msg.tags)
            out = reduce_pb2.ReduceResponse(result=res, window=window)
            await output_queue.put(out)

    def clean_background(self, task):
        """
        Remove the task from the background tasks collection
        """
        self.background_tasks.remove(task)
