# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: sink.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\nsink.proto\x12\x07sink.v1\x1a\x1bgoogle/protobuf/empty.proto\x1a\x1fgoogle/protobuf/timestamp.proto"\xba\x03\n\x0bSinkRequest\x12-\n\x07request\x18\x01 \x01(\x0b\x32\x1c.sink.v1.SinkRequest.Request\x12+\n\x06status\x18\x02 \x01(\x0b\x32\x1b.sink.v1.SinkRequest.Status\x12*\n\thandshake\x18\x03 \x01(\x0b\x32\x12.sink.v1.HandshakeH\x00\x88\x01\x01\x1a\xfd\x01\n\x07Request\x12\x0c\n\x04keys\x18\x01 \x03(\t\x12\r\n\x05value\x18\x02 \x01(\x0c\x12.\n\nevent_time\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12-\n\twatermark\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\n\n\x02id\x18\x05 \x01(\t\x12:\n\x07headers\x18\x06 \x03(\x0b\x32).sink.v1.SinkRequest.Request.HeadersEntry\x1a.\n\x0cHeadersEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a\x15\n\x06Status\x12\x0b\n\x03\x65ot\x18\x01 \x01(\x08\x42\x0c\n\n_handshake"\x18\n\tHandshake\x12\x0b\n\x03sot\x18\x01 \x01(\x08"\x1e\n\rReadyResponse\x12\r\n\x05ready\x18\x01 \x01(\x08"\xbe\x01\n\x0cSinkResponse\x12,\n\x06result\x18\x01 \x01(\x0b\x32\x1c.sink.v1.SinkResponse.Result\x12*\n\thandshake\x18\x02 \x01(\x0b\x32\x12.sink.v1.HandshakeH\x00\x88\x01\x01\x1a\x46\n\x06Result\x12\n\n\x02id\x18\x01 \x01(\t\x12\x1f\n\x06status\x18\x02 \x01(\x0e\x32\x0f.sink.v1.Status\x12\x0f\n\x07\x65rr_msg\x18\x03 \x01(\tB\x0c\n\n_handshake*0\n\x06Status\x12\x0b\n\x07SUCCESS\x10\x00\x12\x0b\n\x07\x46\x41ILURE\x10\x01\x12\x0c\n\x08\x46\x41LLBACK\x10\x02\x32|\n\x04Sink\x12\x39\n\x06SinkFn\x12\x14.sink.v1.SinkRequest\x1a\x15.sink.v1.SinkResponse(\x01\x30\x01\x12\x39\n\x07IsReady\x12\x16.google.protobuf.Empty\x1a\x16.sink.v1.ReadyResponseb\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "sink_pb2", _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _globals["_SINKREQUEST_REQUEST_HEADERSENTRY"]._options = None
    _globals["_SINKREQUEST_REQUEST_HEADERSENTRY"]._serialized_options = b"8\001"
    _globals["_STATUS"]._serialized_start = 781
    _globals["_STATUS"]._serialized_end = 829
    _globals["_SINKREQUEST"]._serialized_start = 86
    _globals["_SINKREQUEST"]._serialized_end = 528
    _globals["_SINKREQUEST_REQUEST"]._serialized_start = 238
    _globals["_SINKREQUEST_REQUEST"]._serialized_end = 491
    _globals["_SINKREQUEST_REQUEST_HEADERSENTRY"]._serialized_start = 445
    _globals["_SINKREQUEST_REQUEST_HEADERSENTRY"]._serialized_end = 491
    _globals["_SINKREQUEST_STATUS"]._serialized_start = 493
    _globals["_SINKREQUEST_STATUS"]._serialized_end = 514
    _globals["_HANDSHAKE"]._serialized_start = 530
    _globals["_HANDSHAKE"]._serialized_end = 554
    _globals["_READYRESPONSE"]._serialized_start = 556
    _globals["_READYRESPONSE"]._serialized_end = 586
    _globals["_SINKRESPONSE"]._serialized_start = 589
    _globals["_SINKRESPONSE"]._serialized_end = 779
    _globals["_SINKRESPONSE_RESULT"]._serialized_start = 695
    _globals["_SINKRESPONSE_RESULT"]._serialized_end = 765
    _globals["_SINK"]._serialized_start = 831
    _globals["_SINK"]._serialized_end = 955
# @@protoc_insertion_point(module_scope)
