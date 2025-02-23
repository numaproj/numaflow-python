from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow._constants import _LOGGER
from pynumaflow.proto.serving import store_pb2_grpc, store_pb2
from pynumaflow.proto.sourcer import source_pb2
from pynumaflow.servingstore._dtypes import ServingStoreCallable, PutDatum, Payload, GetDatum
from pynumaflow.shared.server import handle_async_error
from pynumaflow.types import NumaflowServicerContext


class AsyncServingStoreServicer(store_pb2_grpc.ServingStoreServicer):
    """
    This class is used to create a new grpc Async store servicer instance.
    It implements the ServingStoreServicer interface from the proto store.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(self, serving_store_instance: ServingStoreCallable):
        """Initialize handler methods from the provided serving store handler."""
        self.background_tasks = set()
        self.__serving_store_instance: ServingStoreCallable = serving_store_instance
        self.cleanup_coroutines = []

    async def Put(
            self, request: store_pb2.PutRequest, context: NumaflowServicerContext
    ) -> store_pb2.PutResponse:
        """
        Handles the Put function, processing incoming requests and sending responses.
        """
        # if there is an exception, we will mark all the responses as a failure
        try:
            input_payloads = []
            for x in request.payloads:
                input_payloads.append(Payload(origin=x.origin, value=x.value))
            await self.__serving_store_instance.put(
                datum=PutDatum(id_=request.id, payloads=input_payloads)
            )
        except BaseException as err:
            err_msg = f"Async Serving Store Put: {repr(err)}"
            _LOGGER.critical(err_msg, exc_info=True)
            await handle_async_error(context, err)
        return store_pb2.PutResponse(success=True)

    async def Get(
            self, request: store_pb2.GetRequest, context: NumaflowServicerContext
    ) -> store_pb2.GetResponse:
        """
        Handles the Get function, processing incoming requests and sending responses.
        """
        # if there is an exception, we will mark all the responses as a failure
        try:
            resps = await self.__serving_store_instance.get(datum=GetDatum(id_=request.id))
            resp_payloads = []
            for resp in resps.payloads:
                resp_payloads.append(store_pb2.Payload(origin=resp.origin, value=resp.value))
        except BaseException as err:
            err_msg = f"Async Serving Store Get: {repr(err)}"
            _LOGGER.critical(err_msg, exc_info=True)
            await handle_async_error(context, err)

        return store_pb2.GetResponse(id=request.id, payloads=resp_payloads)

    async def IsReady(
            self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> source_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto source_pb2_grpc.py file.
        """
        return source_pb2.ReadyResponse(ready=True)

