import logging
import multiprocessing
import os


import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    MAP_SOCK_PATH,
)
from pynumaflow.mapper import Datum
from pynumaflow.mapper._dtypes import MapAsyncCallable
from pynumaflow.mapper.proto import map_pb2
from pynumaflow.mapper.proto import map_pb2_grpc
from pynumaflow.types import NumaflowServicerContext
from pynumaflow.info.server import get_sdk_version, write as info_server_write
from pynumaflow.info.types import ServerInfo, Protocol, Language, SERVER_INFO_FILE_PATH

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


class AsyncMapper(map_pb2_grpc.MapServicer):
    """
    Provides an interface to write an Async Mapper
    which will be exposed over gRPC.

    Args:
        handler: Function callable following the type signature of MapCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.mapper import Messages, Message\
    ...     Datum, AsyncMapper
    ... import aiorun
    ...
    >>> async def map_handler(key: [str], datum: Datum) -> Messages:
    ...   val = datum.value
    ...   _ = datum.event_time
    ...   _ = datum.watermark
    ...   messages = Messages(Message(val, keys=keys))
    ...   return messages
    ...
    >>> grpc_server = AsyncMapper(handler=map_handler)
    >>> aiorun.run(grpc_server.start())
    """

    def __init__(
        self,
        handler: MapAsyncCallable,
        sock_path=MAP_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        self.__map_handler: MapAsyncCallable = handler
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

    async def MapFn(
        self, request: map_pb2.MapRequest, context: NumaflowServicerContext
    ) -> map_pb2.MapResponse:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        # proto repeated field(keys) is of type google._upb._message.RepeatedScalarContainer
        # we need to explicitly convert it to list
        try:
            res = await self.__invoke_map(
                list(request.keys),
                Datum(
                    keys=list(request.keys),
                    value=request.value,
                    event_time=request.event_time.ToDatetime(),
                    watermark=request.watermark.ToDatetime(),
                ),
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(e))
            return map_pb2.MapResponse(results=[])

        return map_pb2.MapResponse(results=res)

    async def __invoke_map(self, keys: list[str], req: Datum):
        """
        Invokes the user defined function.
        """
        try:
            msgs = await self.__map_handler(keys, req)
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            raise err
        datums = []
        for msg in msgs:
            datums.append(map_pb2.MapResponse.Result(keys=msg.keys, value=msg.value, tags=msg.tags))

        return datums

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> map_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        return map_pb2.ReadyResponse(ready=True)

    async def __serve_async(self, server) -> None:
        map_pb2_grpc.add_MapServicer_to_server(
            AsyncMapper(handler=self.__map_handler),
            server,
        )
        server.add_insecure_port(self.sock_path)
        _LOGGER.info("gRPC Async Map Server listening on: %s", self.sock_path)
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
        """Starts the Async gRPC mapper on the given UNIX socket."""
        server = grpc.aio.server(options=self._server_options)
        await self.__serve_async(server)
