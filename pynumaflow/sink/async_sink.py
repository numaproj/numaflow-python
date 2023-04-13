import asyncio
from concurrent.futures import ThreadPoolExecutor

import grpc
import logging
import multiprocessing
import os
from google.protobuf import empty_pb2 as _empty_pb2
from typing import Callable, Iterator, Iterable, AsyncIterable

from pynumaflow import setup_logging
from pynumaflow._constants import (
    SINK_SOCK_PATH,
    MAX_MESSAGE_SIZE, DELIMITER, STREAM_EOF,
)
from pynumaflow.function.asynciter import NonBlockingIterator
from pynumaflow.sink import Responses, Datum, Response
from pynumaflow.sink._dtypes import SinkResult
from pynumaflow.sink.proto import udsink_pb2_grpc, udsink_pb2
from pynumaflow.types import NumaflowServicerContext

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

UDSinkCallable = Callable[[Iterator[Datum]], Responses]
_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


def datum_generator(request_iterator: AsyncIterable[udsink_pb2.DatumRequest]) -> AsyncIterable[Datum]:
    async for d in request_iterator:
        datum = Datum(
            keys=list(d.keys),
            sink_msg_id=d.id,
            value=d.value,
            event_time=d.event_time.event_time.ToDatetime(),
            watermark=d.watermark.watermark.ToDatetime(),
        )
        yield datum


class AsyncSink(udsink_pb2_grpc.UserDefinedSinkServicer):
    """
    Provides an interface to write a User Defined Sink (UDSink)
    which will be exposed over gRPC.

    Args:
        sink_handler: Function callable following the type signature of UDSinkCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x 4

    Example invocation:
    >>> from typing import List
    >>> from pynumaflow.sink import Datum, Responses, Response, SyncSink
    >>> def my_handler(datums: Iterator[Datum]) -> Responses:
    ...   responses = Responses()
    ...   for msg in datums:
    ...     responses.append(Response.as_success(msg.id))
    ...   return responses
    >>> grpc_server = SyncSink(my_handler)
    >>> grpc_server.start()
    """

    def __init__(
            self,
            sink_handler: UDSinkCallable,
            sock_path=SINK_SOCK_PATH,
            max_message_size=MAX_MESSAGE_SIZE,
            max_threads=MAX_THREADS,
    ):
        self.background_tasks = set()
        self.__sink_handler: UDSinkCallable = sink_handler
        self.sock_path = f"unix://{sock_path}"
        self._max_message_size = max_message_size
        self._max_threads = max_threads
        self.cleanup_coroutines = []

        self._server_options = [
            ("grpc.max_send_message_length", self._max_message_size),
            ("grpc.max_receive_message_length", self._max_message_size),
        ]

    async def SinkFn(
            self, request_iterator: AsyncIterable[udsink_pb2.DatumRequest], context: NumaflowServicerContext
    ) -> udsink_pb2.ResponseList:
        """
        Applies a sink function to a list of datum elements.
        The pascal case function name comes from the proto udsink_pb2_grpc.py file.
        """
        # if there is an exception, we will mark all the responses as a failure
        datum_iterator = datum_generator(request_iterator=request_iterator)
        response_task = asyncio.create_task(
            self.__async_sink_handler(datum_iterator)
        )

        # Save a reference to the result of this function, to avoid a
        # task disappearing mid-execution.
        self.background_tasks.add(response_task)
        response_task.add_done_callback(lambda t: self.background_tasks.remove(t))

        await response_task
        results_futures = response_task.result()

        for fut in results_futures:
            await fut
            yield udsink_pb2.ResponseList(responses=fut.result())

    async def __async_sink_handler(self, datum_iterator: AsyncIterable[Datum]):
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
                    self.__invoke_sink(riter)
                )
                # Save a reference to the result of this function, to avoid a
                # task disappearing mid-execution.
                self.background_tasks.add(task)
                task.add_done_callback(lambda t: self.background_tasks.remove(t))
                result = SinkResult(task, niter, keys)

                callable_dict[unified_key] = result

            await result.iterator.put(d)

        for unified_key in callable_dict:
            await callable_dict[unified_key].iterator.put(STREAM_EOF)

        tasks = []
        for unified_key in callable_dict:
            fut = callable_dict[unified_key].future
            tasks.append(fut)

        return tasks

    async def __invoke_sink(self, datum_iterator: AsyncIterable[Datum]):
        try:
            rspns = await self.__sink_handler(datum_iterator)
        except Exception as err:
            err_msg = "UDSinkError: %r" % err
            _LOGGER.critical(err_msg, exc_info=True)
            rspns = Responses()
            async for _datum in datum_iterator:
                rspns.append(Response.as_failure(_datum.id, err_msg))
        responses = []
        for rspn in rspns.items():
            responses.append(
                udsink_pb2.Response(id=rspn.id, success=rspn.success, err_msg=rspn.err)
            )
        return responses

    async def IsReady(
            self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> udsink_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto udsink_pb2_grpc.py file.
        """
        return udsink_pb2.ReadyResponse(ready=True)

    async def __serve_async(self, server) -> None:
        udsink_pb2_grpc.add_UserDefinedSinkServicer_to_server(
            AsyncSink(self.__sink_handler), server
        )
        server.add_insecure_port(self.sock_path)
        _LOGGER.info("GRPC Async Server listening on: %s", self.sock_path)
        await server.start()

        async def server_graceful_shutdown():
            _LOGGER.info("Starting graceful shutdown...")
            """
            Shuts down the server with 5 seconds of grace period. During the
            grace period, the server won't accept new connections and allow
            existing RPCs to continue within the grace period.
            await server.stop(5)
            """

        self.cleanup_coroutines.append(server_graceful_shutdown())
        await server.wait_for_termination()

    async def start_async(self) -> None:
        """Starts the Async gRPC server on the given UNIX socket."""
        server = grpc.aio.server(options=self._server_options)
        await self.__serve_async(server)
