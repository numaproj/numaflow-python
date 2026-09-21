from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class NackOptions(_message.Message):
    __slots__ = ("reason", "max_deliveries", "delay", "nack_map")
    class NackMapEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    REASON_FIELD_NUMBER: _ClassVar[int]
    MAX_DELIVERIES_FIELD_NUMBER: _ClassVar[int]
    DELAY_FIELD_NUMBER: _ClassVar[int]
    NACK_MAP_FIELD_NUMBER: _ClassVar[int]
    reason: str
    max_deliveries: int
    delay: int
    nack_map: _containers.ScalarMap[str, str]
    def __init__(self, reason: _Optional[str] = ..., max_deliveries: _Optional[int] = ..., delay: _Optional[int] = ..., nack_map: _Optional[_Mapping[str, str]] = ...) -> None: ...
