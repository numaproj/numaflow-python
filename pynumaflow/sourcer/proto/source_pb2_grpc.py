# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from . import source_pb2 as source__pb2


class SourceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.ReadFn = channel.unary_stream(
            "/source.v1.Source/ReadFn",
            request_serializer=source__pb2.ReadRequest.SerializeToString,
            response_deserializer=source__pb2.ReadResponse.FromString,
        )
        self.AckFn = channel.unary_unary(
            "/source.v1.Source/AckFn",
            request_serializer=source__pb2.AckRequest.SerializeToString,
            response_deserializer=source__pb2.AckResponse.FromString,
        )
        self.PendingFn = channel.unary_unary(
            "/source.v1.Source/PendingFn",
            request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            response_deserializer=source__pb2.PendingResponse.FromString,
        )
        self.IsReady = channel.unary_unary(
            "/source.v1.Source/IsReady",
            request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            response_deserializer=source__pb2.ReadyResponse.FromString,
        )


class SourceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def ReadFn(self, request, context):
        """Read returns a stream of datum responses.
        The size of the returned ReadResponse is less than or equal to the num_records specified in ReadRequest.
        If the request timeout is reached on server side, the returned ReadResponse will contain all the datum that have been read (which could be an empty list).
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def AckFn(self, request, context):
        """AckFn acknowledges a list of datum offsets.
        When AckFn is called, it implicitly indicates that the datum stream has been processed by the source vertex.
        The caller (numa) expects the AckFn to be successful, and it does not expect any errors.
        If there are some irrecoverable errors when the callee (UDSource) is processing the AckFn request,
        then it is best to crash because there are no other retry mechanisms possible.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def PendingFn(self, request, context):
        """PendingFn returns the number of pending records at the user defined source."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def IsReady(self, request, context):
        """IsReady is the heartbeat endpoint for user defined source gRPC."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_SourceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "ReadFn": grpc.unary_stream_rpc_method_handler(
            servicer.ReadFn,
            request_deserializer=source__pb2.ReadRequest.FromString,
            response_serializer=source__pb2.ReadResponse.SerializeToString,
        ),
        "AckFn": grpc.unary_unary_rpc_method_handler(
            servicer.AckFn,
            request_deserializer=source__pb2.AckRequest.FromString,
            response_serializer=source__pb2.AckResponse.SerializeToString,
        ),
        "PendingFn": grpc.unary_unary_rpc_method_handler(
            servicer.PendingFn,
            request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            response_serializer=source__pb2.PendingResponse.SerializeToString,
        ),
        "IsReady": grpc.unary_unary_rpc_method_handler(
            servicer.IsReady,
            request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            response_serializer=source__pb2.ReadyResponse.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler("source.v1.Source", rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class Source(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def ReadFn(
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
        return grpc.experimental.unary_stream(
            request,
            target,
            "/source.v1.Source/ReadFn",
            source__pb2.ReadRequest.SerializeToString,
            source__pb2.ReadResponse.FromString,
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
    def AckFn(
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
            "/source.v1.Source/AckFn",
            source__pb2.AckRequest.SerializeToString,
            source__pb2.AckResponse.FromString,
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
    def PendingFn(
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
            "/source.v1.Source/PendingFn",
            google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            source__pb2.PendingResponse.FromString,
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
            "/source.v1.Source/IsReady",
            google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            source__pb2.ReadyResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
