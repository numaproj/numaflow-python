from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Metadata(_message.Message):
    __slots__ = ("previous_vertex", "sys_metadata", "user_metadata")
    class SysMetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: KeyValueGroup
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[KeyValueGroup, _Mapping]] = ...) -> None: ...
    class UserMetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: KeyValueGroup
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[KeyValueGroup, _Mapping]] = ...) -> None: ...
    PREVIOUS_VERTEX_FIELD_NUMBER: _ClassVar[int]
    SYS_METADATA_FIELD_NUMBER: _ClassVar[int]
    USER_METADATA_FIELD_NUMBER: _ClassVar[int]
    previous_vertex: str
    sys_metadata: _containers.MessageMap[str, KeyValueGroup]
    user_metadata: _containers.MessageMap[str, KeyValueGroup]
    def __init__(self, previous_vertex: _Optional[str] = ..., sys_metadata: _Optional[_Mapping[str, KeyValueGroup]] = ..., user_metadata: _Optional[_Mapping[str, KeyValueGroup]] = ...) -> None: ...

class KeyValueGroup(_message.Message):
    __slots__ = ("key_value",)
    class KeyValueEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    KEY_VALUE_FIELD_NUMBER: _ClassVar[int]
    key_value: _containers.ScalarMap[str, bytes]
    def __init__(self, key_value: _Optional[_Mapping[str, bytes]] = ...) -> None: ...
