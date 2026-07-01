from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class NackOptions(_message.Message):
    __slots__ = ("reason", "max_deliveries", "delay")
    REASON_FIELD_NUMBER: _ClassVar[int]
    MAX_DELIVERIES_FIELD_NUMBER: _ClassVar[int]
    DELAY_FIELD_NUMBER: _ClassVar[int]
    reason: str
    max_deliveries: int
    delay: int
    def __init__(self, reason: _Optional[str] = ..., max_deliveries: _Optional[int] = ..., delay: _Optional[int] = ...) -> None: ...
