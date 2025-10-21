import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from pynumaflow.proto.common import metadata_pb2 as _metadata_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Handshake(_message.Message):
    __slots__ = ("sot",)
    SOT_FIELD_NUMBER: _ClassVar[int]
    sot: bool
    def __init__(self, sot: bool = ...) -> None: ...

class SourceTransformRequest(_message.Message):
    __slots__ = ("request", "handshake")
    class Request(_message.Message):
        __slots__ = ("keys", "value", "event_time", "watermark", "headers", "id", "metadata")
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
        METADATA_FIELD_NUMBER: _ClassVar[int]
        keys: _containers.RepeatedScalarFieldContainer[str]
        value: bytes
        event_time: _timestamp_pb2.Timestamp
        watermark: _timestamp_pb2.Timestamp
        headers: _containers.ScalarMap[str, str]
        id: str
        metadata: _metadata_pb2.Metadata
        def __init__(self, keys: _Optional[_Iterable[str]] = ..., value: _Optional[bytes] = ..., event_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., watermark: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., headers: _Optional[_Mapping[str, str]] = ..., id: _Optional[str] = ..., metadata: _Optional[_Union[_metadata_pb2.Metadata, _Mapping]] = ...) -> None: ...
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    HANDSHAKE_FIELD_NUMBER: _ClassVar[int]
    request: SourceTransformRequest.Request
    handshake: Handshake
    def __init__(self, request: _Optional[_Union[SourceTransformRequest.Request, _Mapping]] = ..., handshake: _Optional[_Union[Handshake, _Mapping]] = ...) -> None: ...

class SourceTransformResponse(_message.Message):
    __slots__ = ("results", "id", "handshake")
    class Result(_message.Message):
        __slots__ = ("keys", "value", "event_time", "tags", "metadata")
        KEYS_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
        TAGS_FIELD_NUMBER: _ClassVar[int]
        METADATA_FIELD_NUMBER: _ClassVar[int]
        keys: _containers.RepeatedScalarFieldContainer[str]
        value: bytes
        event_time: _timestamp_pb2.Timestamp
        tags: _containers.RepeatedScalarFieldContainer[str]
        metadata: _metadata_pb2.Metadata
        def __init__(self, keys: _Optional[_Iterable[str]] = ..., value: _Optional[bytes] = ..., event_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tags: _Optional[_Iterable[str]] = ..., metadata: _Optional[_Union[_metadata_pb2.Metadata, _Mapping]] = ...) -> None: ...
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    HANDSHAKE_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[SourceTransformResponse.Result]
    id: str
    handshake: Handshake
    def __init__(self, results: _Optional[_Iterable[_Union[SourceTransformResponse.Result, _Mapping]]] = ..., id: _Optional[str] = ..., handshake: _Optional[_Union[Handshake, _Mapping]] = ...) -> None: ...

class ReadyResponse(_message.Message):
    __slots__ = ("ready",)
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: bool = ...) -> None: ...
