import asyncio
import logging
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterator

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow._constants import (
    SINK_SOCK_PATH,
    MAX_MESSAGE_SIZE,
)
from pynumaflow.sink import Responses, Datum, Response
from pynumaflow.sink.generated import udsink_pb2_grpc, udsink_pb2
from pynumaflow.types import NumaflowServicerContext

if os.getenv("PYTHONDEBUG"):
    logging.basicConfig(level=logging.DEBUG)

_LOGGER = logging.getLogger(__name__)

UDSinkCallable = Callable[[Iterator[Datum]], Responses]
_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


class UserDefinedSinkServicer(udsink_pb2_grpc.UserDefinedSinkServicer):
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
    >>> from pynumaflow.sink import Datum, Responses, Response, UserDefinedSinkServicer
    >>> def my_handler(datums: Iterator[Datum]) -> Responses:
    ...   responses = Responses()
    ...   for msg in datums:
    ...     responses.append(Response.as_success(msg.id))
    ...   return responses
    >>> grpc_server = UserDefinedSinkServicer(my_handler)
    >>> grpc_server.start()
    """

    def __init__(
        self,
        sink_handler: UDSinkCallable,
        sock_path=SINK_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        self.__sink_handler: UDSinkCallable = sink_handler
        self.sock_path = f"unix://{sock_path}"
        self._max_message_size = max_message_size
        self._max_threads = max_threads
        self._cleanup_coroutines = []

        self._server_options = [
            ("grpc.max_send_message_length", self._max_message_size),
            ("grpc.max_receive_message_length", self._max_message_size),
        ]

    def SinkFn(
        self, request_iterator: Iterator[Datum], context: NumaflowServicerContext
    ) -> udsink_pb2.ResponseList:
        """
        Applies a sink function to a list of datum elements.
        The pascal case function name comes from the generated udsink_pb2_grpc.py file.
        """
        # if there is an exception, we will mark all the responses as a failure
        try:
            rspns = self.__sink_handler(request_iterator)
        except Exception as err:
            err_msg = "UDSinkError: %r" % err
            _LOGGER.critical(err_msg, exc_info=True)
            rspns = Responses()
            for _datum in request_iterator:
                rspns.append(Response.as_failure(_datum.id, err_msg))

        responses = []
        for rspn in rspns.items():
            responses.append(
                udsink_pb2.Response(id=rspn.id, success=rspn.success, err_msg=rspn.err)
            )

        return udsink_pb2.ResponseList(responses=responses)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> udsink_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the generated udsink_pb2_grpc.py file.
        """
        return udsink_pb2.ReadyResponse(ready=True)

    async def __serve_async(self) -> None:
        server = grpc.aio.server(
            ThreadPoolExecutor(max_workers=self._max_threads), options=self._server_options
        )
        udsink_pb2_grpc.add_UserDefinedSinkServicer_to_server(
            UserDefinedSinkServicer(self.__sink_handler), server
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

        self._cleanup_coroutines.append(server_graceful_shutdown())
        await server.wait_for_termination()

    def start_async(self) -> None:
        """Starts the Async gRPC server on the given UNIX socket."""
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self.__serve_async())
        finally:
            loop.run_until_complete(*self._cleanup_coroutines)
            loop.close()

    def start(self) -> None:
        """
        Starts the gRPC server on the given UNIX socket with given max threads.
        """
        server = grpc.server(
            ThreadPoolExecutor(max_workers=self._max_threads), options=self._server_options
        )
        udsink_pb2_grpc.add_UserDefinedSinkServicer_to_server(
            UserDefinedSinkServicer(self.__sink_handler), server
        )
        server.add_insecure_port(self.sock_path)
        server.start()
        _LOGGER.info(
            "GRPC Server listening on: %s with max threads: %s", self.sock_path, self._max_threads
        )
        server.wait_for_termination()
