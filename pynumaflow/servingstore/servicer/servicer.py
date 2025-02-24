from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow._constants import (
    _LOGGER,
)
from pynumaflow.proto.serving import store_pb2_grpc, store_pb2
from pynumaflow.servingstore._dtypes import ServingStoreCallable, Payload, PutDatum, GetDatum
from pynumaflow.shared.server import exit_on_error
from pynumaflow.types import NumaflowServicerContext


class SyncServingStoreServicer(store_pb2_grpc.ServingStoreServicer):
    def __init__(
        self,
        handler: ServingStoreCallable,
    ):
        self.__serving_store_instance: ServingStoreCallable = handler

    def Put(
        self, request: store_pb2.PutRequest, context: NumaflowServicerContext
    ) -> store_pb2.PutResponse:
        """
        Applies a Put function for store request.
        The pascal case function name comes from the proto store_pb2_grpc.py file.
        """
        # if there is an exception, we will mark all the responses as a failure
        try:
            input_payloads = []
            for x in request.payloads:
                input_payloads.append(Payload(origin=x.origin, value=x.value))
            self.__serving_store_instance.put(
                datum=PutDatum(id_=request.id, payloads=input_payloads)
            )
            return store_pb2.PutResponse(success=True)
        except BaseException as err:
            err_msg = f"Serving Store Put: {repr(err)}"
            _LOGGER.critical(err_msg, exc_info=True)
            exit_on_error(context, repr(err))
            return

    def Get(
        self, request: store_pb2.GetRequest, context: NumaflowServicerContext
    ) -> store_pb2.GetResponse:
        """
        Applies a Put function for store request.
        The pascal case function name comes from the proto store_pb2_grpc.py file.
        """
        # if there is an exception, we will mark all the responses as a failure
        try:
            resps = self.__serving_store_instance.get(datum=GetDatum(id_=request.id))
            resp_payloads = []
            for resp in resps.payloads:
                resp_payloads.append(store_pb2.Payload(origin=resp.origin, value=resp.value))
            return store_pb2.GetResponse(id=request.id, payloads=resp_payloads)
        except BaseException as err:
            err_msg = f"Serving Store Get: {repr(err)}"
            _LOGGER.critical(err_msg, exc_info=True)
            exit_on_error(context, repr(err))
            return

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> store_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto store_pb2_grpc.py file.
        """
        return store_pb2.ReadyResponse(ready=True)
