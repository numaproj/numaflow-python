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

class BatchMapRequest(_message.Message):
    __slots__ = ("keys", "value", "event_time", "watermark", "headers", "id")

    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    KEYS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    WATERMARK_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedScalarFieldContainer[str]
    value: bytes
    event_time: _timestamp_pb2.Timestamp
    watermark: _timestamp_pb2.Timestamp
    headers: _containers.ScalarMap[str, str]
    id: str
    def __init__(
        self,
        keys: _Optional[_Iterable[str]] = ...,
        value: _Optional[bytes] = ...,
        event_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...,
        watermark: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...,
        headers: _Optional[_Mapping[str, str]] = ...,
        id: _Optional[str] = ...,
    ) -> None: ...

class BatchMapResponse(_message.Message):
    __slots__ = ("results", "id")

    class Result(_message.Message):
        __slots__ = ("keys", "value", "tags")
        KEYS_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        TAGS_FIELD_NUMBER: _ClassVar[int]
        keys: _containers.RepeatedScalarFieldContainer[str]
        value: bytes
        tags: _containers.RepeatedScalarFieldContainer[str]
        def __init__(
            self,
            keys: _Optional[_Iterable[str]] = ...,
            value: _Optional[bytes] = ...,
            tags: _Optional[_Iterable[str]] = ...,
        ) -> None: ...
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[BatchMapResponse.Result]
    id: str
    def __init__(
        self,
        results: _Optional[_Iterable[_Union[BatchMapResponse.Result, _Mapping]]] = ...,
        id: _Optional[str] = ...,
    ) -> None: ...

class ReadyResponse(_message.Message):
    __slots__ = ("ready",)
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: bool = ...) -> None: ...