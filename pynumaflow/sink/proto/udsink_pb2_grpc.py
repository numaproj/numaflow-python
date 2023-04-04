# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from . import udsink_pb2 as udsink__pb2


class UserDefinedSinkStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.SinkFn = channel.stream_unary(
            "/sink.v1.UserDefinedSink/SinkFn",
            request_serializer=udsink__pb2.Datum.SerializeToString,
            response_deserializer=udsink__pb2.ResponseList.FromString,
        )
        self.IsReady = channel.unary_unary(
            "/sink.v1.UserDefinedSink/IsReady",
            request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            response_deserializer=udsink__pb2.ReadyResponse.FromString,
        )


class UserDefinedSinkServicer(object):
    """Missing associated documentation comment in .proto file."""

    def SinkFn(self, request_iterator, context):
        """SinkFn writes the Datum to a user defined sink."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def IsReady(self, request, context):
        """IsReady is the heartbeat endpoint for gRPC."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_UserDefinedSinkServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "SinkFn": grpc.stream_unary_rpc_method_handler(
            servicer.SinkFn,
            request_deserializer=udsink__pb2.Datum.FromString,
            response_serializer=udsink__pb2.ResponseList.SerializeToString,
        ),
        "IsReady": grpc.unary_unary_rpc_method_handler(
            servicer.IsReady,
            request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            response_serializer=udsink__pb2.ReadyResponse.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "sink.v1.UserDefinedSink", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class UserDefinedSink(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def SinkFn(
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
        return grpc.experimental.stream_unary(
            request_iterator,
            target,
            "/sink.v1.UserDefinedSink/SinkFn",
            udsink__pb2.Datum.SerializeToString,
            udsink__pb2.ResponseList.FromString,
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
            "/sink.v1.UserDefinedSink/IsReady",
            google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            udsink__pb2.ReadyResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
