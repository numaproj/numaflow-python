# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: udsink.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x0cudsink.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1bgoogle/protobuf/empty.proto";\n\tEventTime\x12.\n\nevent_time\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp":\n\tWatermark\x12-\n\twatermark\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp"n\n\x05\x44\x61tum\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x0c\x12\x1e\n\nevent_time\x18\x03 \x01(\x0b\x32\n.EventTime\x12\x1d\n\twatermark\x18\x04 \x01(\x0b\x32\n.Watermark\x12\n\n\x02id\x18\x05 \x01(\t"\x1e\n\rReadyResponse\x12\r\n\x05ready\x18\x01 \x01(\x08"8\n\x08Response\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0f\n\x07success\x18\x02 \x01(\x08\x12\x0f\n\x07\x65rr_msg\x18\x03 \x01(\t",\n\x0cResponseList\x12\x1c\n\tresponses\x18\x01 \x03(\x0b\x32\t.Response2g\n\x0fUserDefinedSink\x12!\n\x06SinkFn\x12\x06.Datum\x1a\r.ResponseList(\x01\x12\x31\n\x07IsReady\x12\x16.google.protobuf.Empty\x1a\x0e.ReadyResponseb\x06proto3'
)


_EVENTTIME = DESCRIPTOR.message_types_by_name["EventTime"]
_WATERMARK = DESCRIPTOR.message_types_by_name["Watermark"]
_DATUM = DESCRIPTOR.message_types_by_name["Datum"]
_READYRESPONSE = DESCRIPTOR.message_types_by_name["ReadyResponse"]
_RESPONSE = DESCRIPTOR.message_types_by_name["Response"]
_RESPONSELIST = DESCRIPTOR.message_types_by_name["ResponseList"]
EventTime = _reflection.GeneratedProtocolMessageType(
    "EventTime",
    (_message.Message,),
    {
        "DESCRIPTOR": _EVENTTIME,
        "__module__": "udsink_pb2"
        # @@protoc_insertion_point(class_scope:EventTime)
    },
)
_sym_db.RegisterMessage(EventTime)

Watermark = _reflection.GeneratedProtocolMessageType(
    "Watermark",
    (_message.Message,),
    {
        "DESCRIPTOR": _WATERMARK,
        "__module__": "udsink_pb2"
        # @@protoc_insertion_point(class_scope:Watermark)
    },
)
_sym_db.RegisterMessage(Watermark)

Datum = _reflection.GeneratedProtocolMessageType(
    "Datum",
    (_message.Message,),
    {
        "DESCRIPTOR": _DATUM,
        "__module__": "udsink_pb2"
        # @@protoc_insertion_point(class_scope:Datum)
    },
)
_sym_db.RegisterMessage(Datum)

ReadyResponse = _reflection.GeneratedProtocolMessageType(
    "ReadyResponse",
    (_message.Message,),
    {
        "DESCRIPTOR": _READYRESPONSE,
        "__module__": "udsink_pb2"
        # @@protoc_insertion_point(class_scope:ReadyResponse)
    },
)
_sym_db.RegisterMessage(ReadyResponse)

Response = _reflection.GeneratedProtocolMessageType(
    "Response",
    (_message.Message,),
    {
        "DESCRIPTOR": _RESPONSE,
        "__module__": "udsink_pb2"
        # @@protoc_insertion_point(class_scope:Response)
    },
)
_sym_db.RegisterMessage(Response)

ResponseList = _reflection.GeneratedProtocolMessageType(
    "ResponseList",
    (_message.Message,),
    {
        "DESCRIPTOR": _RESPONSELIST,
        "__module__": "udsink_pb2"
        # @@protoc_insertion_point(class_scope:ResponseList)
    },
)
_sym_db.RegisterMessage(ResponseList)

_USERDEFINEDSINK = DESCRIPTOR.services_by_name["UserDefinedSink"]
if _descriptor._USE_C_DESCRIPTORS == False:

    DESCRIPTOR._options = None
    _EVENTTIME._serialized_start = 78
    _EVENTTIME._serialized_end = 137
    _WATERMARK._serialized_start = 139
    _WATERMARK._serialized_end = 197
    _DATUM._serialized_start = 199
    _DATUM._serialized_end = 309
    _READYRESPONSE._serialized_start = 311
    _READYRESPONSE._serialized_end = 341
    _RESPONSE._serialized_start = 343
    _RESPONSE._serialized_end = 399
    _RESPONSELIST._serialized_start = 401
    _RESPONSELIST._serialized_end = 445
    _USERDEFINEDSINK._serialized_start = 447
    _USERDEFINEDSINK._serialized_end = 550
# @@protoc_insertion_point(module_scope)
