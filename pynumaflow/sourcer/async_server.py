import logging
import multiprocessing
import os

from collections.abc import AsyncIterable
from google.protobuf import timestamp_pb2 as _timestamp_pb2
import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    SOURCE_SOCK_PATH,
)
from pynumaflow.sourcer import ReadRequest
from pynumaflow.sourcer._dtypes import AsyncSourceReadCallable, Offset, AckRequest
from pynumaflow.sourcer.proto import source_pb2
from pynumaflow.sourcer.proto import source_pb2_grpc
from pynumaflow.types import NumaflowServicerContext
from pynumaflow.info.server import get_sdk_version, write as info_server_write
from pynumaflow.info.types import ServerInfo, Protocol, Language, SERVER_INFO_FILE_PATH

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", "4"))


class AsyncSourcer(source_pb2_grpc.SourceServicer):
    """
    Provides an interface to write an Asynchronous Sourcer
    which will be exposed over gRPC.

    Args:
        read_handler: Function callable following the type signature of AsyncSourceReadCallable
        ack_handler: Function handler for AckFn
        pending_handler: Function handler for PendingFn
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.sourcer import Message \
    ...     ReadRequest, AsyncSourcer
    ... import aiorun
    ... async def read_handler(datum: ReadRequest) -> AsyncIterable[Message]:
    ...     payload = b"payload:test_mock_message"
    ...     keys = ["test_key"]
    ...     offset = mock_offset()
    ...     event_time = mock_event_time()
    ...     for i in range(10):
    ...         yield Message(payload=payload, keys=keys, offset=offset, event_time=event_time)
    ... async def ack_handler(ack_request: AckRequest):
    ...     return
    ... async def pending_handler() -> PendingResponse:
    ...     PendingResponse(count=10)
    >>> grpc_server = AsyncSourcer(read_handler=read_handler,
    ...                     ack_handler=ack_handler,
    ...                     pending_handler=pending_handler)
    >>> aiorun.run(grpc_server.start())
    """

    def __init__(
        self,
        read_handler: AsyncSourceReadCallable,
        ack_handler,
        pending_handler,
        sock_path=SOURCE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        if read_handler is None or ack_handler is None or pending_handler is None:
            raise ValueError("read_handler, ack_handler and pending_handler are required")
        self.__source_read_handler: AsyncSourceReadCallable = read_handler
        self.__source_ack_handler = ack_handler
        self.__source_pending_handler = pending_handler
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

    async def ReadFn(
        self,
        request: source_pb2.ReadRequest,
        context: NumaflowServicerContext,
    ) -> AsyncIterable[source_pb2.ReadResponse]:
        """
        Applies a Read function and returns a stream of datum responses.
        The pascal case function name comes from the proto source_pb2_grpc.py file.
        """

        async for res in self.__invoke_source_read_stream(
            ReadRequest(
                num_records=request.request.num_records,
                timeout_in_ms=request.request.timeout_in_ms,
            )
        ):
            yield source_pb2.ReadResponse(result=res)

    async def __invoke_source_read_stream(self, req: ReadRequest):
        try:
            async for msg in self.__source_read_handler(req):
                event_time_timestamp = _timestamp_pb2.Timestamp()
                event_time_timestamp.FromDatetime(dt=msg.event_time)
                yield source_pb2.ReadResponse.Result(
                    payload=msg.payload,
                    keys=msg.keys,
                    offset=msg.offset.as_dict,
                    event_time=event_time_timestamp,
                )
        except Exception as err:
            _LOGGER.critical("User-Defined Source ReadError ", exc_info=True)
            raise err

    async def AckFn(
        self, request: source_pb2.AckRequest, context: NumaflowServicerContext
    ) -> source_pb2.AckResponse:
        """
        Applies an Ack function in User Defined Source
        """
        # proto repeated field(offsets) is of type google._upb._message.RepeatedScalarContainer
        # we need to explicitly convert it to list
        offsets = []
        for offset in request.request.offsets:
            offsets.append(Offset(offset.offset, offset.partition_id))
        try:
            await self.__invoke_ack(ack_req=request)
        except Exception as e:
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(e))
            raise e

        return source_pb2.AckResponse()

    async def __invoke_ack(self, ack_req: AckRequest):
        """
        Invokes the Source Ack Function.
        """
        try:
            await self.__source_ack_handler(ack_req)
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            raise err
        return source_pb2.AckResponse.Result()

    async def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> source_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto source_pb2_grpc.py file.
        """
        return source_pb2.ReadyResponse(ready=True)

    async def PendingFn(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> source_pb2.PendingResponse:
        """
        PendingFn returns the number of pending records
        at the user defined source.
        """
        try:
            count = await self.__source_pending_handler()
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            raise err
        resp = source_pb2.PendingResponse.Result(count=count.count)
        return source_pb2.PendingResponse(result=resp)

    async def __serve_async(self, server) -> None:
        source_pb2_grpc.add_sourceServicer_to_server(
            AsyncSourcer(read_handler=self.__source_read_handler),
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
