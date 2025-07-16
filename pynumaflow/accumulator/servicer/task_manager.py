import asyncio
from collections.abc import AsyncIterable
from datetime import datetime
from typing import Union
import logging

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
    The key name is used to identify the Reduce task.
    The format is: start_time:end_time:key1:key2:...
    """
    return f"{DELIMITER.join(keys)}"


def build_window_hash(window):
    """
    Builds a hash for the given window.
    The hash is used to identify the Reduce Window
    The format is: start_time:end_time
    """
    return f"{window.start.ToMilliseconds()}:{window.end.ToMilliseconds()}"


def create_window_eof_response(window):
    """Create a Reduce response with EOF=True for a given window"""
    return accumulator_pb2.ReduceResponse(window=window, EOF=True)


class TaskManager:
    """
    TaskManager is responsible for managing the Reduce tasks.
    It is created whenever a new reduce operation is requested.
    """

    def __init__(self, handler: Union[AccumulatorAsyncCallable, _AccumulatorBuilderClass]):
        # A dictionary to store the task information
        self.tasks: dict[str, AccumulatorResult] = {}
        # Collection for storing strong references to all running tasks.
        # Event loop only keeps a weak reference, which can cause it to
        # get lost during execution.
        self.background_tasks = set()
        # Handler for the reduce operation
        self.__accumulator_handler = handler
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
            # the results of the reduce operation can be sent to the
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
            # Reduce handler with the given keys, request iterator, and window.
            task = asyncio.create_task(self.__invoke_accumulator(riter, res_queue))
            # Save a reference to the result of this function, to avoid a
            # task disappearing mid-execution.
            self.background_tasks.add(task)
            task.add_done_callback(self.clean_background)

            # Create a new AccumulatorResult object to store the task information
            curr_task = AccumulatorResult(
                task, niter, keys, res_queue, consumer, datetime.fromtimestamp(-1)
            )

            # Save the result of the reduce operation to the task list
            self.tasks[unified_key] = curr_task

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
        Invokes the UDF reduce handler with the given keys,
        request iterator, and window. Returns the result of the
        reduce operation.
        """
        new_instance = self.__accumulator_handler

        # If the accumulator handler is a class instance, create a new instance of it.
        # It is required for a new key to be processed by a
        # new instance of the reducer for a given window
        # Otherwise the function handler can be called directly
        if isinstance(self.__accumulator_handler, _AccumulatorBuilderClass):
            new_instance = self.__accumulator_handler.create()
        try:
            _ = await new_instance(request_iterator, output)
            # send EOF to the output stream
            await output.put(STREAM_EOF)
        # If there is an error in the reduce operation, log and
        # then send the error to the result queue
        except BaseException as err:
            _LOGGER.critical("panic inside accumulator handle", exc_info=True)
            logging.info(f"[ACCUMULATOR_DEBUG] Exception caught in __invoke_accumulator: {err}")
            logging.info(f"[ACCUMULATOR_DEBUG] Putting exception in global_result_queue: {repr(err)}")
            # Put the exception in the result queue
            await self.global_result_queue.put(err)
            logging.info(f"[ACCUMULATOR_DEBUG] Exception put in global_result_queue successfully")

    async def process_input_stream(
        self, request_iterator: AsyncIterable[accumulator_pb2.AccumulatorRequest]
    ):
        # Start iterating through the request iterator and create tasks
        # based on the operation type received.
        logging.info(f"[PROCESS_INPUT_DEBUG] Starting process_input_stream")
        try:
            request_count = 0
            async for request in request_iterator:
                request_count += 1
                logging.info(f"[PROCESS_INPUT_DEBUG] Processing request {request_count}, operation: {request.operation}")
                logging.info(f"[PROCESS_INPUT_DEBUG] Operation value: {request.operation}")
                logging.info(f"[PROCESS_INPUT_DEBUG] WindowOperation.OPEN: {int(WindowOperation.OPEN)}")
                logging.info(f"[PROCESS_INPUT_DEBUG] WindowOperation.APPEND: {int(WindowOperation.APPEND)}")
                logging.info(f"[PROCESS_INPUT_DEBUG] WindowOperation.CLOSE: {int(WindowOperation.CLOSE)}")
                logging.info(f"[PROCESS_INPUT_DEBUG] Comparison - request.operation is int(WindowOperation.OPEN): {request.operation is int(WindowOperation.OPEN)}")
                # check whether the request is an open or append operation
                if request.operation is int(WindowOperation.OPEN):
                    # create a new task for the open operation and
                    # put the request in the task iterator
                    logging.info(f"[PROCESS_INPUT_DEBUG] Creating task for OPEN operation")
                    await self.create_task(request)
                elif request.operation is int(WindowOperation.APPEND):
                    # append the task data to the existing task
                    # if the task does not exist, create a new task
                    logging.info(f"[PROCESS_INPUT_DEBUG] Sending datum to task for APPEND operation")
                    await self.send_datum_to_task(request)
                elif request.operation is int(WindowOperation.CLOSE):
                    # close the current task for req
                    logging.info(f"[PROCESS_INPUT_DEBUG] Closing task for CLOSE operation")
                    await self.close_task(request)
                else:
                    logging.info(f"[PROCESS_INPUT_DEBUG] No operation matched")
            logging.info(f"[PROCESS_INPUT_DEBUG] Finished processing {request_count} requests")
        # If there is an error in the reduce operation, log and
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

            # get the list of reduce tasks that are currently being processed
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

            # # Once all tasks are completed, send EOF to all windows that
            # # were processed in the Task Manager. We send a single
            # # EOF message per window.
            # current_windows = self.get_unique_windows()
            # for window in current_windows.values():
            #     # Send an EOF message to the global result queue
            #     # This will signal that window has been processed
            #     eof_window_msg = create_window_eof_response(window=window)
            #     await self.global_result_queue.put(eof_window_msg)

            # Once all tasks are completed, senf EOF the global result queue
            await self.global_result_queue.put(STREAM_EOF)
        except BaseException as e:
            err_msg = f"Reduce Streaming Error: {repr(e)}"
            _LOGGER.critical(err_msg, exc_info=True)
            await self.global_result_queue.put(e)

    async def write_to_global_queue(
        self, input_queue: NonBlockingIterator, output_queue: NonBlockingIterator, unified_key: str
    ):
        """
        This task is for given Reduce task.
        This would from the local result queue for the task and then write
        to the global result queue
        """
        reader = input_queue.read_iterator()
        task = self.tasks[unified_key]

        _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Starting write_to_global_queue for key: {unified_key}")
        _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Task: {task}")
        _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Task watermark: {task.latest_watermark}")
        _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Task keys: {task.keys}")
        
        # Store the last datum processed for this task - we need this for watermark info
        last_datum = getattr(task, 'last_datum', None)
        _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Last datum: {last_datum}")
        
        wm: datetime = task.latest_watermark
        async for msg in reader:
            _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Received message: {msg}")
            _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Message type: {type(msg)}")
            _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Message keys: {msg.keys}")
            _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Message value: {msg.value}")
            _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Message tags: {msg.tags}")
            
            # For now, let's see if we can get the datum info from the task
            # The task should have access to the datum information
            
            # If we have a last_datum, use its watermark and metadata
            if last_datum:
                _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Using last_datum watermark: {last_datum.watermark}")
                _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Using last_datum event_time: {last_datum.event_time}")
                _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Using last_datum headers: {last_datum.headers}")
                _LOGGER.info(f"[WRITE_TO_GLOBAL_DEBUG] Using last_datum id: {last_datum.id}")
                
                # Update watermark if the datum's watermark is newer
                if wm < last_datum.watermark:
                    task.update_watermark(last_datum.watermark)
                    self.tasks[unified_key] = task
                    wm = last_datum.watermark
                    
                start_dt = datetime.fromtimestamp(0)
                end_dt = wm
                res = accumulator_pb2.AccumulatorResponse(
                    payload=accumulator_pb2.Payload(
                        keys=msg.keys,
                        value=msg.value,
                        event_time=last_datum.event_time,
                        watermark=last_datum.watermark,
                        headers=last_datum.headers,
                        id=last_datum.id,
                    ),
                    window=accumulator_pb2.KeyedWindow(
                        start=start_dt, end=end_dt, slot="slot-0", keys=task.keys
                    ),
                    EOF=False,
                    tags=msg.tags,
                )
                await output_queue.put(res)
            else:
                _LOGGER.error("[WRITE_TO_GLOBAL_DEBUG] No last_datum available!")
                # This is the problematic code that tries to access msg.watermark
                # TODO: We need to fix this by storing datum information properly
                start_dt = datetime.fromtimestamp(0)
                end_dt = wm
                res = accumulator_pb2.AccumulatorResponse(
                    payload=accumulator_pb2.Payload(
                        keys=msg.keys,
                        value=msg.value,
                        event_time=datetime.now(),  # Temporary fallback
                        watermark=wm,  # Use task watermark as fallback
                        headers={},  # Empty headers as fallback
                        id="",  # Empty id as fallback
                    ),
                    window=accumulator_pb2.KeyedWindow(
                        start=start_dt, end=end_dt, slot="slot-0", keys=task.keys
                    ),
                    EOF=False,
                    tags=msg.tags,
                )
                await output_queue.put(res)
        # send EOF
        res = accumulator_pb2.AccumulatorResponse(
            window=accumulator_pb2.KeyedWindow(
                start=datetime.fromtimestamp(0), end=wm, slot="slot-0", keys=task.keys
            ),
            EOF=True,
        )
        await output_queue.put(res)

    def clean_background(self, task):
        """
        Remove the task from the background tasks collection
        """
        self.background_tasks.remove(task)
