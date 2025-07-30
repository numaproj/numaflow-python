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

class MapRequest(_message.Message):
    __slots__ = ("request", "id", "handshake", "status")

    class Request(_message.Message):
        __slots__ = ("keys", "value", "event_time", "watermark", "headers")

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
        keys: _containers.RepeatedScalarFieldContainer[str]
        value: bytes
        event_time: _timestamp_pb2.Timestamp
        watermark: _timestamp_pb2.Timestamp
        headers: _containers.ScalarMap[str, str]
        def __init__(
            self,
            keys: _Optional[_Iterable[str]] = ...,
            value: _Optional[bytes] = ...,
            event_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...,
            watermark: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...,
            headers: _Optional[_Mapping[str, str]] = ...,
        ) -> None: ...
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    HANDSHAKE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    request: MapRequest.Request
    id: str
    handshake: Handshake
    status: TransmissionStatus
    def __init__(
        self,
        request: _Optional[_Union[MapRequest.Request, _Mapping]] = ...,
        id: _Optional[str] = ...,
        handshake: _Optional[_Union[Handshake, _Mapping]] = ...,
        status: _Optional[_Union[TransmissionStatus, _Mapping]] = ...,
    ) -> None: ...

class Handshake(_message.Message):
    __slots__ = ("sot",)
    SOT_FIELD_NUMBER: _ClassVar[int]
    sot: bool
    def __init__(self, sot: bool = ...) -> None: ...

class TransmissionStatus(_message.Message):
    __slots__ = ("eot",)
    EOT_FIELD_NUMBER: _ClassVar[int]
    eot: bool
    def __init__(self, eot: bool = ...) -> None: ...

class MapResponse(_message.Message):
    __slots__ = ("results", "id", "handshake", "status")

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
    HANDSHAKE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[MapResponse.Result]
    id: str
    handshake: Handshake
    status: TransmissionStatus
    def __init__(
        self,
        results: _Optional[_Iterable[_Union[MapResponse.Result, _Mapping]]] = ...,
        id: _Optional[str] = ...,
        handshake: _Optional[_Union[Handshake, _Mapping]] = ...,
        status: _Optional[_Union[TransmissionStatus, _Mapping]] = ...,
    ) -> None: ...

class ReadyResponse(_message.Message):
    __slots__ = ("ready",)
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: bool = ...) -> None: ...
