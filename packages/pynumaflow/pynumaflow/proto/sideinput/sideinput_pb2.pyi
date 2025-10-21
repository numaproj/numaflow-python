from google.protobuf import empty_pb2 as _empty_pb2
from pynumaflow.proto.common import metadata_pb2 as _metadata_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SideInputResponse(_message.Message):
    __slots__ = ("value", "no_broadcast", "metadata")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    NO_BROADCAST_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    no_broadcast: bool
    metadata: _metadata_pb2.Metadata
    def __init__(self, value: _Optional[bytes] = ..., no_broadcast: bool = ..., metadata: _Optional[_Union[_metadata_pb2.Metadata, _Mapping]] = ...) -> None: ...

class ReadyResponse(_message.Message):
    __slots__ = ("ready",)
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: bool = ...) -> None: ...
