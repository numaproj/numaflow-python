from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from typing import (
    ClassVar as _ClassVar,
    Optional as _Optional,
)

DESCRIPTOR: _descriptor.FileDescriptor

class ReadyResponse(_message.Message):
    __slots__ = ["ready"]
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool

    def __init__(self, ready: _Optional[bool] = ...) -> None: ...

class SideInputResponse(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(
        self,
        value: _Optional[bytes],
    ) -> None: ...
