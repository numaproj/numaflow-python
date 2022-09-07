from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReadyResponse(_message.Message):
    __slots__ = ["ready"]
    READY_FIELD_NUMBER: _ClassVar[bool]
    ready: bool
    def __init__(self, ready: _Optional[bool] = ...) -> None: ...
