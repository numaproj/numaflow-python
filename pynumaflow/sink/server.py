import asyncio
import logging
from os import environ


from google.protobuf import empty_pb2 as _empty_pb2

import grpc
from typing import Callable, Any, Iterator, List

from pynumaflow._constants import (
    SINK_SOCK_PATH,
    DATUM_KEY,
)
from pynumaflow.sink import Response, Responses, Datum
from pynumaflow.sink.generated import udsink_pb2_grpc, udsink_pb2
from pynumaflow.types import NumaflowServicerContext

if environ.get("PYTHONDEBUG"):
    logging.basicConfig(level=logging.DEBUG)

_LOGGER = logging.getLogger(__name__)

UDSinkCallable = Callable[[List[Datum], Any], Responses]


class UserDefinedSinkServicer(udsink_pb2_grpc.UserDefinedSinkServicer):
    """
    Provides an interface to write a User Defined Sink (UDSink)
    which will be exposed over gRPC.

    Args:
        sink_handler: Function callable following the type signature of UDSinkCallable
        sock_path: Path to the UNIX Domain Socket

    Example invocation:
    >>> from typing import List
    >>> from pynumaflow.sink import Datum, Responses, Response, UserDefinedSinkServicer
    >>> def udsink_handler(datums: List[Datum]) -> Responses:
    ...   responses = Responses()
    ...   for msg in datums:
    ...     print("User Defined Sink", msg)
    ...     responses.append(Response.as_success(msg.id))
    ...   return responses
    >>> grpc_server = UserDefinedSinkServicer(udsink_handler)
    >>> grpc_server.start()
    """

    def __init__(self, sink_handler: UDSinkCallable, sock_path=SINK_SOCK_PATH):
        self.__sink_handler: UDSinkCallable = sink_handler
        self.sock_path = sock_path
        self._cleanup_coroutines = []

    def SinkFn(
        self, request: udsink_pb2.DatumList, context: NumaflowServicerContext
    ) -> udsink_pb2.ResponseList:
        """
        Applies a sink function to a list of datum elements.
        The pascal case function name comes from the generated udsink_pb2_grpc.py file.
        """

        msgs = self.__sink_handler(request.elements)

        responses = []
        for msg in msgs.items():
            responses.append(udsink_pb2.Response(id=msg.id, success=msg.success, err_msg=msg.err))

        return udsink_pb2.ResponseList(responses=responses)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> udsink_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the generated udsink_pb2_grpc.py file.
        """
        return udsink_pb2.ReadyResponse(ready=True)

    async def __serve(self) -> None:
        server = grpc.aio.server()
        udsink_pb2_grpc.add_UserDefinedSinkServicer_to_server(
            UserDefinedSinkServicer(self.__sink_handler), server
        )
        server.add_insecure_port(self.sock_path)
        _LOGGER.info("Server listening on: %s", self.sock_path)
        await server.start()

        async def server_graceful_shutdown():
            logging.info("Starting graceful shutdown...")
            # Shuts down the server with 5 seconds of grace period. During the
            # grace period, the server won't accept new connections and allow
            # existing RPCs to continue within the grace period.
            await server.stop(5)

        self._cleanup_coroutines.append(server_graceful_shutdown())
        await server.wait_for_termination()

    def start(self) -> None:
        """Starts the server on the given UNIX socket."""
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self.__serve())
        finally:
            loop.run_until_complete(*self._cleanup_coroutines)
            loop.close()
