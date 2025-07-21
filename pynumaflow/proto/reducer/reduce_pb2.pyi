from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
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

class ReduceRequest(_message.Message):
    __slots__ = ("payload", "operation")

    class WindowOperation(_message.Message):
        __slots__ = ("event", "windows")

        class Event(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            OPEN: _ClassVar[ReduceRequest.WindowOperation.Event]
            CLOSE: _ClassVar[ReduceRequest.WindowOperation.Event]
            APPEND: _ClassVar[ReduceRequest.WindowOperation.Event]
        OPEN: ReduceRequest.WindowOperation.Event
        CLOSE: ReduceRequest.WindowOperation.Event
        APPEND: ReduceRequest.WindowOperation.Event
        EVENT_FIELD_NUMBER: _ClassVar[int]
        WINDOWS_FIELD_NUMBER: _ClassVar[int]
        event: ReduceRequest.WindowOperation.Event
        windows: _containers.RepeatedCompositeFieldContainer[Window]
        def __init__(
            self,
            event: _Optional[_Union[ReduceRequest.WindowOperation.Event, str]] = ...,
            windows: _Optional[_Iterable[_Union[Window, _Mapping]]] = ...,
        ) -> None: ...

    class Payload(_message.Message):
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
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    payload: ReduceRequest.Payload
    operation: ReduceRequest.WindowOperation
    def __init__(
        self,
        payload: _Optional[_Union[ReduceRequest.Payload, _Mapping]] = ...,
        operation: _Optional[_Union[ReduceRequest.WindowOperation, _Mapping]] = ...,
    ) -> None: ...

class Window(_message.Message):
    __slots__ = ("start", "end", "slot")
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    SLOT_FIELD_NUMBER: _ClassVar[int]
    start: _timestamp_pb2.Timestamp
    end: _timestamp_pb2.Timestamp
    slot: str
    def __init__(
        self,
        start: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...,
        end: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...,
        slot: _Optional[str] = ...,
    ) -> None: ...

class ReduceResponse(_message.Message):
    __slots__ = ("result", "window", "EOF")

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
    RESULT_FIELD_NUMBER: _ClassVar[int]
    WINDOW_FIELD_NUMBER: _ClassVar[int]
    EOF_FIELD_NUMBER: _ClassVar[int]
    result: ReduceResponse.Result
    window: Window
    EOF: bool
    def __init__(
        self,
        result: _Optional[_Union[ReduceResponse.Result, _Mapping]] = ...,
        window: _Optional[_Union[Window, _Mapping]] = ...,
        EOF: bool = ...,
    ) -> None: ...

class ReadyResponse(_message.Message):
    __slots__ = ("ready",)
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: bool = ...) -> None: ...
