# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from . import store_pb2 as store__pb2


class ServingStoreStub(object):
    """ServingStore defines a set of methods to interface with a user-defined Store."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Put = channel.unary_unary(
            "/serving.v1.ServingStore/Put",
            request_serializer=store__pb2.PutRequest.SerializeToString,
            response_deserializer=store__pb2.PutResponse.FromString,
        )
        self.Get = channel.unary_unary(
            "/serving.v1.ServingStore/Get",
            request_serializer=store__pb2.GetRequest.SerializeToString,
            response_deserializer=store__pb2.GetResponse.FromString,
        )
        self.IsReady = channel.unary_unary(
            "/serving.v1.ServingStore/IsReady",
            request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            response_deserializer=store__pb2.ReadyResponse.FromString,
        )


class ServingStoreServicer(object):
    """ServingStore defines a set of methods to interface with a user-defined Store."""

    def Put(self, request, context):
        """Put is to put the PutRequest into the Store."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def Get(self, request, context):
        """Get gets the GetRequest from the Store."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def IsReady(self, request, context):
        """IsReady checks the health of the container interfacing the Store."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_ServingStoreServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "Put": grpc.unary_unary_rpc_method_handler(
            servicer.Put,
            request_deserializer=store__pb2.PutRequest.FromString,
            response_serializer=store__pb2.PutResponse.SerializeToString,
        ),
        "Get": grpc.unary_unary_rpc_method_handler(
            servicer.Get,
            request_deserializer=store__pb2.GetRequest.FromString,
            response_serializer=store__pb2.GetResponse.SerializeToString,
        ),
        "IsReady": grpc.unary_unary_rpc_method_handler(
            servicer.IsReady,
            request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            response_serializer=store__pb2.ReadyResponse.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "serving.v1.ServingStore", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class ServingStore(object):
    """ServingStore defines a set of methods to interface with a user-defined Store."""

    @staticmethod
    def Put(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/serving.v1.ServingStore/Put",
            store__pb2.PutRequest.SerializeToString,
            store__pb2.PutResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def Get(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/serving.v1.ServingStore/Get",
            store__pb2.GetRequest.SerializeToString,
            store__pb2.GetResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def IsReady(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/serving.v1.ServingStore/IsReady",
            google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            store__pb2.ReadyResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
