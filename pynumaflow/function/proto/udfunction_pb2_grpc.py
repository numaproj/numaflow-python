# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from . import udfunction_pb2 as udfunction__pb2


class UserDefinedFunctionStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.MapFn = channel.unary_unary(
            "/function.v1.UserDefinedFunction/MapFn",
            request_serializer=udfunction__pb2.Datum.SerializeToString,
            response_deserializer=udfunction__pb2.DatumList.FromString,
        )
        self.MapTFn = channel.unary_unary(
            "/function.v1.UserDefinedFunction/MapTFn",
            request_serializer=udfunction__pb2.Datum.SerializeToString,
            response_deserializer=udfunction__pb2.DatumList.FromString,
        )
        self.ReduceFn = channel.stream_stream(
            "/function.v1.UserDefinedFunction/ReduceFn",
            request_serializer=udfunction__pb2.Datum.SerializeToString,
            response_deserializer=udfunction__pb2.DatumList.FromString,
        )
        self.IsReady = channel.unary_unary(
            "/function.v1.UserDefinedFunction/IsReady",
            request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            response_deserializer=udfunction__pb2.ReadyResponse.FromString,
        )


class UserDefinedFunctionServicer(object):
    """Missing associated documentation comment in .proto file."""

    def MapFn(self, request, context):
        """MapFn applies a function to each datum element."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def MapTFn(self, request, context):
        """MapTFn applies a function to each datum element.
        In addition to map function, MapTFn also supports assigning a new event time to datum.
        MapTFn can be used only at source vertex by source data transformer.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def ReduceFn(self, request_iterator, context):
        """ReduceFn applies a reduce function to a datum stream."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def IsReady(self, request, context):
        """IsReady is the heartbeat endpoint for gRPC."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_UserDefinedFunctionServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "MapFn": grpc.unary_unary_rpc_method_handler(
            servicer.MapFn,
            request_deserializer=udfunction__pb2.Datum.FromString,
            response_serializer=udfunction__pb2.DatumList.SerializeToString,
        ),
        "MapTFn": grpc.unary_unary_rpc_method_handler(
            servicer.MapTFn,
            request_deserializer=udfunction__pb2.Datum.FromString,
            response_serializer=udfunction__pb2.DatumList.SerializeToString,
        ),
        "ReduceFn": grpc.stream_stream_rpc_method_handler(
            servicer.ReduceFn,
            request_deserializer=udfunction__pb2.Datum.FromString,
            response_serializer=udfunction__pb2.DatumList.SerializeToString,
        ),
        "IsReady": grpc.unary_unary_rpc_method_handler(
            servicer.IsReady,
            request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            response_serializer=udfunction__pb2.ReadyResponse.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "function.v1.UserDefinedFunction", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class UserDefinedFunction(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def MapFn(
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
            "/function.v1.UserDefinedFunction/MapFn",
            udfunction__pb2.Datum.SerializeToString,
            udfunction__pb2.DatumList.FromString,
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
    def MapTFn(
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
            "/function.v1.UserDefinedFunction/MapTFn",
            udfunction__pb2.Datum.SerializeToString,
            udfunction__pb2.DatumList.FromString,
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
    def ReduceFn(
        request_iterator,
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
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/function.v1.UserDefinedFunction/ReduceFn",
            udfunction__pb2.Datum.SerializeToString,
            udfunction__pb2.DatumList.FromString,
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
            "/function.v1.UserDefinedFunction/IsReady",
            google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            udfunction__pb2.ReadyResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
