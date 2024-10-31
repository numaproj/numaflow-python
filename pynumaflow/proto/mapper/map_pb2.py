# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: map.proto
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
    b'\n\tmap.proto\x12\x06map.v1\x1a\x1bgoogle/protobuf/empty.proto\x1a\x1fgoogle/protobuf/timestamp.proto"\xac\x03\n\nMapRequest\x12+\n\x07request\x18\x01 \x01(\x0b\x32\x1a.map.v1.MapRequest.Request\x12\n\n\x02id\x18\x02 \x01(\t\x12)\n\thandshake\x18\x03 \x01(\x0b\x32\x11.map.v1.HandshakeH\x00\x88\x01\x01\x12/\n\x06status\x18\x04 \x01(\x0b\x32\x1a.map.v1.TransmissionStatusH\x01\x88\x01\x01\x1a\xef\x01\n\x07Request\x12\x0c\n\x04keys\x18\x01 \x03(\t\x12\r\n\x05value\x18\x02 \x01(\x0c\x12.\n\nevent_time\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12-\n\twatermark\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x38\n\x07headers\x18\x05 \x03(\x0b\x32\'.map.v1.MapRequest.Request.HeadersEntry\x1a.\n\x0cHeadersEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x42\x0c\n\n_handshakeB\t\n\x07_status"\x18\n\tHandshake\x12\x0b\n\x03sot\x18\x01 \x01(\x08"!\n\x12TransmissionStatus\x12\x0b\n\x03\x65ot\x18\x01 \x01(\x08"\xf0\x01\n\x0bMapResponse\x12+\n\x07results\x18\x01 \x03(\x0b\x32\x1a.map.v1.MapResponse.Result\x12\n\n\x02id\x18\x02 \x01(\t\x12)\n\thandshake\x18\x03 \x01(\x0b\x32\x11.map.v1.HandshakeH\x00\x88\x01\x01\x12/\n\x06status\x18\x04 \x01(\x0b\x32\x1a.map.v1.TransmissionStatusH\x01\x88\x01\x01\x1a\x33\n\x06Result\x12\x0c\n\x04keys\x18\x01 \x03(\t\x12\r\n\x05value\x18\x02 \x01(\x0c\x12\x0c\n\x04tags\x18\x03 \x03(\tB\x0c\n\n_handshakeB\t\n\x07_status"\x1e\n\rReadyResponse\x12\r\n\x05ready\x18\x01 \x01(\x08\x32u\n\x03Map\x12\x34\n\x05MapFn\x12\x12.map.v1.MapRequest\x1a\x13.map.v1.MapResponse(\x01\x30\x01\x12\x38\n\x07IsReady\x12\x16.google.protobuf.Empty\x1a\x15.map.v1.ReadyResponseB7Z5github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1b\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "map_pb2", _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    _globals["DESCRIPTOR"]._options = None
    _globals[
        "DESCRIPTOR"
    ]._serialized_options = b"Z5github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"
    _globals["_MAPREQUEST_REQUEST_HEADERSENTRY"]._options = None
    _globals["_MAPREQUEST_REQUEST_HEADERSENTRY"]._serialized_options = b"8\001"
    _globals["_MAPREQUEST"]._serialized_start = 84
    _globals["_MAPREQUEST"]._serialized_end = 512
    _globals["_MAPREQUEST_REQUEST"]._serialized_start = 248
    _globals["_MAPREQUEST_REQUEST"]._serialized_end = 487
    _globals["_MAPREQUEST_REQUEST_HEADERSENTRY"]._serialized_start = 441
    _globals["_MAPREQUEST_REQUEST_HEADERSENTRY"]._serialized_end = 487
    _globals["_HANDSHAKE"]._serialized_start = 514
    _globals["_HANDSHAKE"]._serialized_end = 538
    _globals["_TRANSMISSIONSTATUS"]._serialized_start = 540
    _globals["_TRANSMISSIONSTATUS"]._serialized_end = 573
    _globals["_MAPRESPONSE"]._serialized_start = 576
    _globals["_MAPRESPONSE"]._serialized_end = 816
    _globals["_MAPRESPONSE_RESULT"]._serialized_start = 740
    _globals["_MAPRESPONSE_RESULT"]._serialized_end = 791
    _globals["_READYRESPONSE"]._serialized_start = 818
    _globals["_READYRESPONSE"]._serialized_end = 848
    _globals["_MAP"]._serialized_start = 850
    _globals["_MAP"]._serialized_end = 967
# @@protoc_insertion_point(module_scope)
