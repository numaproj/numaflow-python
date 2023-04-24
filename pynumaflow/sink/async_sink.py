import logging
import multiprocessing
import os
from typing import Callable, Iterator, AsyncIterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    SINK_SOCK_PATH,
    MAX_MESSAGE_SIZE,
)
from pynumaflow.info.server import get_sdk_version, write as info_server_write
from pynumaflow.info.types import ServerInfo, Protocol, Language, SERVER_INFO_FILE_PATH
from pynumaflow.sink import Responses, Datum, Response
from pynumaflow.sink.proto import udsink_pb2_grpc, udsink_pb2
from pynumaflow.types import NumaflowServicerContext

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

UDSinkCallable = Callable[[Iterator[Datum]], Responses]
_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


async def datum_generator(
    request_iterator: AsyncIterable[udsink_pb2.DatumRequest],
) -> AsyncIterable[Datum]:
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
    which will be exposed over an Asyncronous gRPC server.

    Args:
        sink_handler: Function callable following the type signature of UDSinkCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x 4

    Example invocation:
    >>> from typing import List
    >>> from pynumaflow.sink import Datum, Responses, Response, Sink
    >>> async def my_handler(datums: AsyncIterable[Datum]) -> Responses:
    ...   responses = Responses()
    ...   async for msg in datums:
    ...     responses.append(Response.as_success(msg.id))
    ...   return responses
    >>> grpc_server = AsyncSink(my_handler)
    >>> aiorun.run(grpc_server.start())
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
        self,
        request_iterator: AsyncIterable[udsink_pb2.DatumRequest],
        context: NumaflowServicerContext,
    ) -> udsink_pb2.ResponseList:
        """
        Applies a sink function to a list of datum elements.
        The pascal case function name comes from the proto udsink_pb2_grpc.py file.
        """
        # if there is an exception, we will mark all the responses as a failure
        datum_iterator = datum_generator(request_iterator=request_iterator)
        results = await self.__invoke_sink(datum_iterator)

        return udsink_pb2.ResponseList(responses=results)

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
        serv_info = ServerInfo(
            protocol=Protocol.UDS,
            language=Language.PYTHON,
            version=get_sdk_version(),
        )
        info_server_write(server_info=serv_info, info_file=SERVER_INFO_FILE_PATH)

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

    async def start(self) -> None:
        """Starts the Async gRPC server on the given UNIX socket."""
        server = grpc.aio.server(options=self._server_options)
        await self.__serve_async(server)
