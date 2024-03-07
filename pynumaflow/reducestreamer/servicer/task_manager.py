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


def get_unique_key(keys, window):
    return f"{window.start.ToMilliseconds()}:{window.end.ToMilliseconds()}:{DELIMITER.join(keys)}"


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
        self.result_queue = NonBlockingIterator()
        _LOGGER.info("MYDEBUG: TM CREATED FN")

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
        _LOGGER.info("MYDEBUG: GOT CRE")
        # if len of windows in request != 1, raise error
        if len(req.windows) != 1:
            raise UDFError("reduce create operation error: invalid number of windows")

        d = req.payload
        keys = d.keys()
        unified_key = get_unique_key(keys, req.windows[0])
        result = self.tasks.get(unified_key, None)

        # If the task does not exist, create a new task
        if not result:
            niter = NonBlockingIterator()
            riter = niter.read_iterator()
            res_queue = NonBlockingIterator()

            consumer = asyncio.create_task(
                self.consumer(res_queue, self.result_queue, req.windows[0])
            )
            # Save a reference to the result of this function, to avoid a
            # task disappearing mid-execution.
            self.background_tasks.add(consumer)
            consumer.add_done_callback(self.clean_background)

            task = asyncio.create_task(self.__invoke_reduce(keys, riter, res_queue, req.windows[0]))
            # Save a reference to the result of this function, to avoid a
            # task disappearing mid-execution.
            self.background_tasks.add(task)
            task.add_done_callback(self.clean_background)
            result = ReduceResult(task, niter, keys, req.windows[0], res_queue, consumer)

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
        unified_key = get_unique_key(keys, req.windows[0])
        result = self.tasks.get(unified_key, None)
        if not result:
            await self.create_task(req)
        else:
            # print(result)
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
        _LOGGER.info("MYDEBUG: INVOKE RED")

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
            _LOGGER.info("MYDEBUG: DONE REQ")
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            await self.result_queue.put(err)
            # raise err

    async def producer(self, request_iterator: AsyncIterable[reduce_pb2.ReduceRequest]):
        # Start iterating through the request iterator and create tasks
        # based on the operation type received.
        _LOGGER.info("MYDEBUG: IN PROD FN")
        try:
            async for request in request_iterator:
                _LOGGER.info("MYDEBUG: GOT REQ")
                # check whether the request is an open or append operation
                if request.operation is int(WindowOperation.OPEN):
                    # create a new task for the open operation
                    await self.create_task(request)
                elif request.operation is int(WindowOperation.APPEND):
                    # append the task data to the existing task
                    await self.append_task(request)
        except Exception as e:
            _LOGGER.critical("Reduce streaming error", exc_info=True)
            await self.result_queue.put(e)

        # send EOF to all the tasks once the request iterator is exhausted
        # This will signal the tasks to stop reading the data on their
        # respective iterators.
        await self.stream_send_eof()

        # get the results from all the tasks
        res = self.get_tasks()

        _LOGGER.info("MYDEBUG: GERRE")

        try:
            # iterate through the tasks and yield the response
            # once the task is completed.
            for task in res:
                _LOGGER.info("MYDEBUG: GERRE-0")

                fut = task.future
                await fut
                _LOGGER.info("MYDEBUG: GERRE-1")

                # con_future = task.consumer_future
                # await con_future
                # for msg in fut.result():
                #     yield reduce_pb2.ReduceResponse(result=msg, window=task.window)
                await task.result_queue.put(STREAM_EOF)
                _LOGGER.info("MYDEBUG: GERRE-2")

                con_future = task.consumer_future
                await con_future
                _LOGGER.info("MYDEBUG: GERRE-3")

                # print("RES", task.window)
                await self.result_queue.put(task.window)
        except Exception as e:
            await self.result_queue.put(e)
            # raise e

        # Once all tasks are completed, close the consumer queue
        await self.result_queue.put(STREAM_EOF)

    async def consumer(
        self, input_queue: NonBlockingIterator, output_queue: NonBlockingIterator, window
    ):
        reader = input_queue.read_iterator()
        async for msg in reader:
            res = reduce_pb2.ReduceResponse.Result(keys=msg.keys, value=msg.value, tags=msg.tags)
            out = reduce_pb2.ReduceResponse(result=res, window=window)
            await output_queue.put(out)

    def clean_background(self, task):
        # print("Clean", task)
        _LOGGER.info("MYDEBUG: CLEAN")
        self.background_tasks.remove(task)
