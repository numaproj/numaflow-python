import asyncio
from collections.abc import AsyncIterable
from datetime import datetime
from typing import Union

from google.protobuf import timestamp_pb2
from pynumaflow._constants import (
    STREAM_EOF,
    DELIMITER,
    _LOGGER,
)
from pynumaflow.accumulator._dtypes import (
    AccumulatorResult,
    Datum,
    _AccumulatorBuilderClass,
    AccumulatorAsyncCallable,
    WindowOperation,
)
from pynumaflow.proto.accumulator import accumulator_pb2
from pynumaflow.shared.asynciter import NonBlockingIterator


def build_unique_key_name(keys):
    """
    Builds a unique key name for the given keys and window.
    The key name is used to identify the Accumulator task.
    The format is: start_time:end_time:key1:key2:...
    """
    return f"{DELIMITER.join(keys)}"


def build_window_hash(window):
    """
    Builds a hash for the given window.
    The hash is used to identify the Accumulator Window
    The format is: start_time:end_time
    """
    return f"{window.start.ToMilliseconds()}:{window.end.ToMilliseconds()}"


def create_window_eof_response(window):
    """Create a Accumulator response with EOF=True for a given window"""
    return accumulator_pb2.AccumulatorResponse(window=window, EOF=True)


class TaskManager:
    """
    TaskManager is responsible for managing the Accumulator tasks.
    It is created whenever a new accumulator operation is requested.
    """

    def __init__(self, handler: Union[AccumulatorAsyncCallable, _AccumulatorBuilderClass]):
        # A dictionary to store the task information
        self.tasks: dict[str, AccumulatorResult] = {}
        # Collection for storing strong references to all running tasks.
        # Event loop only keeps a weak reference, which can cause it to
        # get lost during execution.
        self.background_tasks = set()
        # Handler for the accumulator operation
        self.__accumulator_handler = handler
        # Queue to store the results of the accumulator operation
        # This queue is used to send the results to the client
        # once the accumulator operation is completed.
        # This queue is also used to send the error/exceptions to the client
        # if the accumulator operation fails.
        self.global_result_queue = NonBlockingIterator()
        # EOF response counting to ensure proper termination
        self._expected_eof_count = 0
        self._received_eof_count = 0
        self._eof_count_lock = asyncio.Lock()
        self._stream_termination_event = asyncio.Event()

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
        Returns the list of accumulator tasks that are
        currently being processed
        """
        return list(self.tasks.values())

    async def stream_send_eof(self):
        """
        Sends EOF to input streams of all the Accumulator
        tasks that are currently being processed.
        This is called when the input grpc stream is closed.
        """
        # Create a copy of the keys to avoid dictionary size change during iteration
        task_keys = list(self.tasks.keys())
        for unified_key in task_keys:
            await self.tasks[unified_key].iterator.put(STREAM_EOF)
            self.tasks.pop(unified_key)

    async def close_task(self, req):
        d = req.payload
        keys = d.keys()
        unified_key = build_unique_key_name(keys)
        curr_task = self.tasks.get(unified_key, None)

        if curr_task:
            await self.tasks[unified_key].iterator.put(STREAM_EOF)
            self.tasks.pop(unified_key)
        else:
            _LOGGER.critical("accumulator task not found", exc_info=True)
            err = BaseException("accumulator task not found")
            # Put the exception in the result queue
            await self.global_result_queue.put(err)

    async def create_task(self, req):
        """
        Creates a new accumulator task for the given request.
        Based on the request we compute a unique key, and then
        it creates a new task or appends the request to the existing task.
        """
        d = req.payload
        keys = d.keys()
        unified_key = build_unique_key_name(keys)
        curr_task = self.tasks.get(unified_key, None)

        # If the task does not exist, create a new task
        if not curr_task:
            niter = NonBlockingIterator()
            riter = niter.read_iterator()
            # Create a new result queue for the current task
            # We create a new result queue for each task, so that
            # the results of the accumulator operation can be sent to the
            # the global result queue, which in turn sends the results
            # to the client.
            res_queue = NonBlockingIterator()

            # Create a new write_to_global_queue task for the current, this will read from the
            # result queue specifically for the current task and update the
            # global result queue
            consumer = asyncio.create_task(
                self.write_to_global_queue(res_queue, self.global_result_queue, unified_key)
            )
            # Save a reference to the result of this function, to avoid a
            # task disappearing mid-execution.
            self.background_tasks.add(consumer)
            consumer.add_done_callback(self.clean_background)

            # Create a new task for the accumulator operation, this will invoke the
            # Accumulator handler with the given keys, request iterator, and window.
            task = asyncio.create_task(self.__invoke_accumulator(riter, res_queue))
            # Save a reference to the result of this function, to avoid a
            # task disappearing mid-execution.
            self.background_tasks.add(task)
            task.add_done_callback(self.clean_background)

            # Create a new AccumulatorResult object to store the task information
            curr_task = AccumulatorResult(
                task, niter, keys, res_queue, consumer, datetime.fromtimestamp(-1)
            )

            # Save the result of the accumulator operation to the task list
            self.tasks[unified_key] = curr_task

            # Increment expected EOF count since we created a new task
            async with self._eof_count_lock:
                self._expected_eof_count += 1

        # Put the request in the iterator
        await curr_task.iterator.put(d)

    async def send_datum_to_task(self, req):
        """
        Appends the request to the existing window reduce task.
        If the task does not exist, create it.
        """
        d = req.payload
        keys = d.keys()
        unified_key = build_unique_key_name(keys)
        result = self.tasks.get(unified_key, None)
        if not result:
            await self.create_task(req)
        else:
            await result.iterator.put(d)

    async def __invoke_accumulator(
        self,
        request_iterator: AsyncIterable[Datum],
        output: NonBlockingIterator,
    ):
        """
        Invokes the UDF accumulator handler with the given keys,
        request iterator, and window. Returns the result of the
        accumulator operation.
        """
        new_instance = self.__accumulator_handler

        # If the accumulator handler is a class instance, create a new instance of it.
        # It is required for a new key to be processed by a
        # new instance of the accumulator for a given window
        # Otherwise the function handler can be called directly
        if isinstance(self.__accumulator_handler, _AccumulatorBuilderClass):
            new_instance = self.__accumulator_handler.create()
        try:
            _ = await new_instance(request_iterator, output)
            # send EOF to the output stream
            await output.put(STREAM_EOF)
        # If there is an error in the accumulator operation, log and
        # then send the error to the result queue
        except BaseException as err:
            _LOGGER.critical("panic inside accumulator handle", exc_info=True)
            # Put the exception in the result queue
            await self.global_result_queue.put(err)

    async def process_input_stream(
        self, request_iterator: AsyncIterable[accumulator_pb2.AccumulatorRequest]
    ):
        # Start iterating through the request iterator and create tasks
        # based on the operation type received.
        try:
            request_count = 0
            async for request in request_iterator:
                request_count += 1
                # check whether the request is an open or append operation
                if request.operation is int(WindowOperation.OPEN):
                    # create a new task for the open operation and
                    # put the request in the task iterator
                    await self.create_task(request)
                elif request.operation is int(WindowOperation.APPEND):
                    # append the task data to the existing task
                    # if the task does not exist, create a new task
                    await self.send_datum_to_task(request)
                elif request.operation is int(WindowOperation.CLOSE):
                    # close the current task for req
                    await self.close_task(request)
                else:
                    _LOGGER.debug(f"No operation matched for request: {request}", exc_info=True)

        # If there is an error in the accumulator operation, log and
        # then send the error to the result queue
        except BaseException as e:
            err_msg = f"Accumulator Error: {repr(e)}"
            _LOGGER.critical(err_msg, exc_info=True)
            # Put the exception in the global result queue
            await self.global_result_queue.put(e)
            return

        try:
            # send EOF to all the tasks once the request iterator is exhausted
            # This will signal the tasks to stop reading the data on their
            # respective iterators.
            await self.stream_send_eof()

            # get the list of accumulator tasks that are currently being processed
            # iterate through the tasks and wait for them to complete
            for task in self.get_tasks():
                # Once this is done, we know that the task has written all the results
                # to the local result queue
                fut = task.future
                await fut

                # # Send an EOF message to the local result queue
                # # This will signal that the task has completed processing
                await task.result_queue.put(STREAM_EOF)

                # Wait for the local queue to write
                # all the results of this task to the global result queue
                con_future = task.consumer_future
                await con_future

            # Wait for all tasks to send their EOF responses before terminating the stream
            # This ensures proper ordering: all messages -> all EOF responses -> STREAM_EOF
            await self._stream_termination_event.wait()

            # Now send STREAM_EOF to terminate the global result queue iterator
            await self.global_result_queue.put(STREAM_EOF)
        except BaseException as e:
            err_msg = f"Accumulator Streaming Error: {repr(e)}"
            _LOGGER.critical(err_msg, exc_info=True)
            await self.global_result_queue.put(e)

    async def write_to_global_queue(
        self, input_queue: NonBlockingIterator, output_queue: NonBlockingIterator, unified_key: str
    ):
        """
        This task is for given Accumulator task.
        This would from the local result queue for the task and then write
        to the global result queue
        """
        reader = input_queue.read_iterator()
        task = self.tasks[unified_key]

        wm: datetime = task.latest_watermark
        async for msg in reader:
            # Convert the window to a datetime object
            # Only update watermark if msg.watermark is not None
            if msg.watermark is not None and wm < msg.watermark:
                task.update_watermark(msg.watermark)
                self.tasks[unified_key] = task
                wm = msg.watermark

            # Convert datetime to protobuf timestamp
            event_time_pb = timestamp_pb2.Timestamp()
            if msg.event_time is not None:
                event_time_pb.FromDatetime(msg.event_time)

            watermark_pb = timestamp_pb2.Timestamp()
            if msg.watermark is not None:
                watermark_pb.FromDatetime(msg.watermark)

            start_dt_pb = timestamp_pb2.Timestamp()
            start_dt_pb.FromDatetime(datetime.fromtimestamp(0))

            end_dt_pb = timestamp_pb2.Timestamp()
            end_dt_pb.FromDatetime(wm)

            res = accumulator_pb2.AccumulatorResponse(
                payload=accumulator_pb2.Payload(
                    keys=msg.keys,
                    value=msg.value,
                    event_time=event_time_pb,
                    watermark=watermark_pb,
                    headers=msg.headers,
                    id=msg.id,
                ),
                window=accumulator_pb2.KeyedWindow(
                    start=start_dt_pb, end=end_dt_pb, slot="slot-0", keys=task.keys
                ),
                EOF=False,
                tags=msg.tags,
            )
            await output_queue.put(res)
        # send EOF
        start_eof_pb = timestamp_pb2.Timestamp()
        start_eof_pb.FromDatetime(datetime.fromtimestamp(0))

        end_eof_pb = timestamp_pb2.Timestamp()
        end_eof_pb.FromDatetime(wm)

        res = accumulator_pb2.AccumulatorResponse(
            window=accumulator_pb2.KeyedWindow(
                start=start_eof_pb, end=end_eof_pb, slot="slot-0", keys=task.keys
            ),
            EOF=True,
        )
        await output_queue.put(res)

        # Increment received EOF count and check if all tasks are done
        async with self._eof_count_lock:
            self._received_eof_count += 1

            # Check if all tasks have sent their EOF responses
            if self._received_eof_count == self._expected_eof_count:
                self._stream_termination_event.set()
            elif self._received_eof_count > self._expected_eof_count:
                # Still set the event to prevent hanging, but log the error
                self._stream_termination_event.set()

    def clean_background(self, task):
        """
        Remove the task from the background tasks collection
        """
        self.background_tasks.remove(task)
