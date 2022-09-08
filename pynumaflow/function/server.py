import asyncio
import logging

from pynumaflow.function import udfunction_pb2
from pynumaflow.function import udfunction_pb2_grpc

import grpc
from typing import Callable, Any

from pynumaflow._constants import (
    FUNCTION_SOCK_PATH,
    DATUM_KEY,
)
from pynumaflow.function import Messages

_LOGGER = logging.getLogger(__name__)

UDFMapCallable = Callable[[str, bytes, Any], Messages]


class UserDefinedFunctionServicer(udfunction_pb2_grpc.UserDefinedFunctionServicer):

    def __init__(self, map_handler: UDFMapCallable, sock_path=FUNCTION_SOCK_PATH):
        self.__map_handler: UDFMapCallable = map_handler
        self.sock_path = sock_path
        self._cleanup_coroutines = []

    def MapFn(self, request: udfunction_pb2.Datum, context):
        """Applies a function to each datum element.
        """
        key = ""
        value = request.value
        for metadata_key, metadata_value in context.invocation_metadata():
            if metadata_key == DATUM_KEY:
                key = metadata_value

        msgs = self.__map_handler(key+"_test", value)

        datum_list = []
        for msg in msgs.items():
            print(msg)
            datum_list.append(udfunction_pb2.Datum(key=msg.key, value=msg.value))

        return udfunction_pb2.DatumList(elements=datum_list)

    def ReduceFn(self, request_iterator, context):
        """Applies a reduce function to a datum stream.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def IsReady(self, request, context):
        """IsReady is the heartbeat endpoint for gRPC.
        """
        return udfunction_pb2.ReadyResponse(ready=True)

    async def serve(self) -> None:
        _ = self.sock_path
        uds_addresses = ['unix:///tmp/numaflow-test.sock']
        server = grpc.aio.server()
        udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(UserDefinedFunctionServicer(self.__map_handler), server)
        for uds_address in uds_addresses:
            server.add_insecure_port(uds_address)
            _LOGGER.info('Server listening on: %s', uds_address)
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
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self.serve())
        finally:
            loop.run_until_complete(*self._cleanup_coroutines)
            loop.close()
