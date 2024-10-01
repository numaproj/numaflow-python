from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class SideInputResponse(_message.Message):
    __slots__ = ("value", "no_broadcast")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    NO_BROADCAST_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    no_broadcast: bool
    def __init__(self, value: _Optional[bytes] = ..., no_broadcast: bool = ...) -> None: ...

class ReadyResponse(_message.Message):
    __slots__ = ("ready",)
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: bool = ...) -> None: ...
