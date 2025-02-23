from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import (
    ClassVar as _ClassVar,
    Iterable as _Iterable,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

DESCRIPTOR: _descriptor.FileDescriptor

class Payload(_message.Message):
    __slots__ = ("origin", "value")
    ORIGIN_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    origin: str
    value: bytes
    def __init__(self, origin: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class PutRequest(_message.Message):
    __slots__ = ("id", "payloads")
    ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOADS_FIELD_NUMBER: _ClassVar[int]
    id: str
    payloads: _containers.RepeatedCompositeFieldContainer[Payload]
    def __init__(
        self,
        id: _Optional[str] = ...,
        payloads: _Optional[_Iterable[_Union[Payload, _Mapping]]] = ...,
    ) -> None: ...

class PutResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("id", "payloads")
    ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOADS_FIELD_NUMBER: _ClassVar[int]
    id: str
    payloads: _containers.RepeatedCompositeFieldContainer[Payload]
    def __init__(
        self,
        id: _Optional[str] = ...,
        payloads: _Optional[_Iterable[_Union[Payload, _Mapping]]] = ...,
    ) -> None: ...

class ReadyResponse(_message.Message):
    __slots__ = ("ready",)
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: bool = ...) -> None: ...
