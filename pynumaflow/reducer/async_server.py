import asyncio
import logging
import multiprocessing
import os

from datetime import datetime, timezone
from collections.abc import AsyncIterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    WIN_START_TIME,
    WIN_END_TIME,
    MAX_MESSAGE_SIZE,
    STREAM_EOF,
    DELIMITER,
    REDUCE_SOCK_PATH,
)
from pynumaflow.reducer import Datum, IntervalWindow, Metadata
from pynumaflow.reducer._dtypes import ReduceResult, ReduceCallable
from pynumaflow.reducer.asynciter import NonBlockingIterator
from pynumaflow.reducer.proto import reduce_pb2
from pynumaflow.reducer.proto import reduce_pb2_grpc
from pynumaflow.types import NumaflowServicerContext
from pynumaflow.info.server import get_sdk_version, write as info_server_write
from pynumaflow.info.types import ServerInfo, Protocol, Language, SERVER_INFO_FILE_PATH

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


async def datum_generator(
    request_iterator: AsyncIterable[reduce_pb2.ReduceRequest],
) -> AsyncIterable[Datum]:
    async for d in request_iterator:
        datum = Datum(
            keys=list(d.keys),
            value=d.value,
            event_time=d.event_time.ToDatetime(),
            watermark=d.watermark.ToDatetime(),
        )
        yield datum


class AsyncReducer(reduce_pb2_grpc.ReduceServicer):
    """
    Provides an interface to write a Reduce Function
    which will be exposed over gRPC.

    Args:
        handler: Function callable following the type signature of ReduceCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.reducer import Messages, Message\
    ...     Datum, Metadata, AsyncReducer
    ... import aiorun
    ...
    >>> async def reduce_handler(key: list[str], datums: AsyncIterable[Datum],
    >>> md: Metadata) -> Messages:
    ...   interval_window = md.interval_window
    ...   counter = 0
    ...   async for _ in datums:
    ...     counter += 1
    ...   msg = (
    ...       f"counter:{counter} interval_window_start:{interval_window.start} "
    ...       f"interval_window_end:{interval_window.end}"
    ...   )
    ...   return Messages(Message(value=str.encode(msg), keys=keys))
    ...
    >>> grpc_server = AsyncReducer(handler=reduce_handler)
    >>> aiorun.run(grpc_server.start())
    """

    def __init__(
        self,
        handler: ReduceCallable,
        sock_path=REDUCE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        self.__reduce_handler: ReduceCallable = handler
        self.sock_path = f"unix://{sock_path}"
        self._max_message_size = max_message_size
        self._max_threads = max_threads
        self.cleanup_coroutines = []
        # Collection for storing strong references to all running tasks.
        # Event loop only keeps a weak reference, which can cause it to
        # get lost during execution.
        self.background_tasks = set()

        self._server_options = [
            ("grpc.max_send_message_length", self._max_message_size),
            ("grpc.max_receive_message_length", self._max_message_size),
        ]

    async def ReduceFn(
        self,
        request_iterator: AsyncIterable[reduce_pb2.ReduceRequest],
        context: NumaflowServicerContext,
    ) -> reduce_pb2.ReduceResponse:
        """
        Applies a reduce function to a datum stream.
        The pascal case function name comes from the proto reduce_pb2_grpc.py file.
        """

        start, end = None, None
        for metadata_key, metadata_value in context.invocation_metadata():
            if metadata_key == WIN_START_TIME:
                start = metadata_value
            elif metadata_key == WIN_END_TIME:
                end = metadata_value
        if not (start or end):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"Expected to have all key/window_start_time/window_end_time; "
                f"got start: {start}, end: {end}."
            )
            yield reduce_pb2.ReduceResponse(results=[])
            return

        start_dt = datetime.fromtimestamp(int(start) / 1e3, timezone.utc)
        end_dt = datetime.fromtimestamp(int(end) / 1e3, timezone.utc)
        interval_window = IntervalWindow(start=start_dt, end=end_dt)

        datum_iterator = datum_generator(request_iterator=request_iterator)

        response_task = asyncio.create_task(
            self.__async_reduce_handler(interval_window, datum_iterator)
        )

        # Save a reference to the result of this function, to avoid a
        # task disappearing mid-execution.
        self.background_tasks.add(response_task)
        response_task.add_done_callback(lambda t: self.background_tasks.remove(t))

        await response_task
        results_futures = response_task.result()

        try:
            for fut in results_futures:
                await fut
                yield reduce_pb2.ReduceResponse(results=fut.result())
        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(e.__str__())
            yield reduce_pb2.ReduceResponse(results=[])

    async def __async_reduce_handler(self, interval_window, datum_iterator: AsyncIterable[Datum]):
        callable_dict = {}
        # iterate through all the values
        async for d in datum_iterator:
            keys = d.keys()
            unified_key = DELIMITER.join(keys)
            result = callable_dict.get(unified_key, None)

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
                result = ReduceResult(task, niter, keys)

                callable_dict[unified_key] = result

            await result.iterator.put(d)

        for unified_key in callable_dict:
            await callable_dict[unified_key].iterator.put(STREAM_EOF)

        tasks = []
        for unified_key in callable_dict:
            fut = callable_dict[unified_key].future
            tasks.append(fut)

        return tasks

    async def __invoke_reduce(
        self, keys: list[str], request_iterator: AsyncIterable[Datum], md: Metadata
    ):
        try:
            msgs = await self.__reduce_handler(keys, request_iterator, md)
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

    async def __serve_async(self, server) -> None:
        reduce_pb2_grpc.add_ReduceServicer_to_server(
            AsyncReducer(handler=self.__reduce_handler),
            server,
        )
        server.add_insecure_port(self.sock_path)
        _LOGGER.info("GRPC Async Server listening on: %s", self.sock_path)
        await server.start()
        serv_info = ServerInfo(
            protocol=Protocol.UDS,
            language=Language.PYTHON,
            version=get_sdk_version(),
        )
        info_server_write(server_info=serv_info, info_file=SERVER_INFO_FILE_PATH)

        async def server_graceful_shutdown():
            """
            Shuts down the server with 5 seconds of grace period. During the
            grace period, the server won't accept new connections and allow
            existing RPCs to continue within the grace period.
            """
            _LOGGER.info("Starting graceful shutdown...")
            await server.stop(5)

        self.cleanup_coroutines.append(server_graceful_shutdown())
        await server.wait_for_termination()

    async def start(self) -> None:
        """Starts the Async gRPC server on the given UNIX socket."""
        server = grpc.aio.server(options=self._server_options)
        await self.__serve_async(server)
