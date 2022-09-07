import asyncio
import logging
import udfunction_pb2
import udfunction_pb2_grpc

import grpc

_LOGGER = logging.getLogger(__name__)


class UserDefinedFunctionServicer(udfunction_pb2_grpc.UserDefinedFunctionServicer):

    def MapFn(self, request, context):
        """Applies a function to each datum element.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

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


async def serve() -> None:
    uds_addresses = ['unix:///tmp/numaflow-test.sock']
    server = grpc.aio.server()
    udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(UserDefinedFunctionServicer(), server)
    for uds_address in uds_addresses:
        server.add_insecure_port(uds_address)
        _LOGGER.info('Server listening on: %s', uds_address)
    await server.start()
    await server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())
