# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: udfunction.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b"\n\x10udfunction.proto\x12\x19pynumaflow.function.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1bgoogle"
    b'/protobuf/empty.proto";\n\tEventTime\x12.\n\nevent_time\x18\x01 \x01('
    b'\x0b\x32\x1a.google.protobuf.Timestamp":\n\tWatermark\x12-\n\twatermark\x18\x01 \x01('
    b'\x0b\x32\x1a.google.protobuf.Timestamp"\x96\x01\n\x05\x44\x61tum\x12\x0b\n\x03key\x18\x01 \x01('
    b"\t\x12\r\n\x05value\x18\x02 \x01(\x0c\x12\x38\n\nevent_time\x18\x03 \x01("
    b"\x0b\x32$.pynumaflow.function.proto.EventTime\x12\x37\n\twatermark\x18\x04 \x01("
    b'\x0b\x32$.pynumaflow.function.proto.Watermark"?\n\tDatumList\x12\x32\n\x08\x65lements\x18\x01 \x03(\x0b\x32 '
    b'.pynumaflow.function.proto.Datum"\x1e\n\rReadyResponse\x12\r\n\x05ready\x18\x01 \x01('
    b"\x08\x32\xdd\x02\n\x13UserDefinedFunction\x12O\n\x05MapFn\x12 "
    b".pynumaflow.function.proto.Datum\x1a$.pynumaflow.function.proto.DatumList\x12P\n\x06MapTFn\x12 "
    b".pynumaflow.function.proto.Datum\x1a$.pynumaflow.function.proto.DatumList\x12V\n\x08ReduceFn\x12 "
    b".pynumaflow.function.proto.Datum\x1a$.pynumaflow.function.proto.DatumList("
    b"\x01\x30\x01\x12K\n\x07IsReady\x12\x16.google.protobuf.Empty\x1a("
    b".pynumaflow.function.proto.ReadyResponseb\x06proto3"
)


_EVENTTIME = DESCRIPTOR.message_types_by_name["EventTime"]
_WATERMARK = DESCRIPTOR.message_types_by_name["Watermark"]
_DATUM = DESCRIPTOR.message_types_by_name["Datum"]
_DATUMLIST = DESCRIPTOR.message_types_by_name["DatumList"]
_READYRESPONSE = DESCRIPTOR.message_types_by_name["ReadyResponse"]
EventTime = _reflection.GeneratedProtocolMessageType(
    "EventTime",
    (_message.Message,),
    {
        "DESCRIPTOR": _EVENTTIME,
        "__module__": "udfunction_pb2"
        # @@protoc_insertion_point(class_scope:pynumaflow.function.proto.EventTime)
    },
)
_sym_db.RegisterMessage(EventTime)

Watermark = _reflection.GeneratedProtocolMessageType(
    "Watermark",
    (_message.Message,),
    {
        "DESCRIPTOR": _WATERMARK,
        "__module__": "udfunction_pb2"
        # @@protoc_insertion_point(class_scope:pynumaflow.function.proto.Watermark)
    },
)
_sym_db.RegisterMessage(Watermark)

Datum = _reflection.GeneratedProtocolMessageType(
    "Datum",
    (_message.Message,),
    {
        "DESCRIPTOR": _DATUM,
        "__module__": "udfunction_pb2"
        # @@protoc_insertion_point(class_scope:pynumaflow.function.proto.Datum)
    },
)
_sym_db.RegisterMessage(Datum)

DatumList = _reflection.GeneratedProtocolMessageType(
    "DatumList",
    (_message.Message,),
    {
        "DESCRIPTOR": _DATUMLIST,
        "__module__": "udfunction_pb2"
        # @@protoc_insertion_point(class_scope:pynumaflow.function.proto.DatumList)
    },
)
_sym_db.RegisterMessage(DatumList)

ReadyResponse = _reflection.GeneratedProtocolMessageType(
    "ReadyResponse",
    (_message.Message,),
    {
        "DESCRIPTOR": _READYRESPONSE,
        "__module__": "udfunction_pb2"
        # @@protoc_insertion_point(class_scope:pynumaflow.function.proto.ReadyResponse)
    },
)
_sym_db.RegisterMessage(ReadyResponse)

_USERDEFINEDFUNCTION = DESCRIPTOR.services_by_name["UserDefinedFunction"]
if _descriptor._USE_C_DESCRIPTORS == False:

    DESCRIPTOR._options = None
    _EVENTTIME._serialized_start = 109
    _EVENTTIME._serialized_end = 168
    _WATERMARK._serialized_start = 170
    _WATERMARK._serialized_end = 228
    _DATUM._serialized_start = 231
    _DATUM._serialized_end = 381
    _DATUMLIST._serialized_start = 383
    _DATUMLIST._serialized_end = 446
    _READYRESPONSE._serialized_start = 448
    _READYRESPONSE._serialized_end = 478
    _USERDEFINEDFUNCTION._serialized_start = 481
    _USERDEFINEDFUNCTION._serialized_end = 830
# @@protoc_insertion_point(module_scope)
