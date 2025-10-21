import datetime

from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from pynumaflow.proto.common import metadata_pb2 as _metadata_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Payload(_message.Message):
    __slots__ = ("keys", "value", "event_time", "watermark", "id", "headers", "metadata")
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
    ID_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedScalarFieldContainer[str]
    value: bytes
    event_time: _timestamp_pb2.Timestamp
    watermark: _timestamp_pb2.Timestamp
    id: str
    headers: _containers.ScalarMap[str, str]
    metadata: _metadata_pb2.Metadata
    def __init__(self, keys: _Optional[_Iterable[str]] = ..., value: _Optional[bytes] = ..., event_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., watermark: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., id: _Optional[str] = ..., headers: _Optional[_Mapping[str, str]] = ..., metadata: _Optional[_Union[_metadata_pb2.Metadata, _Mapping]] = ...) -> None: ...

class AccumulatorRequest(_message.Message):
    __slots__ = ("payload", "operation")
    class WindowOperation(_message.Message):
        __slots__ = ("event", "keyedWindow")
        class Event(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            OPEN: _ClassVar[AccumulatorRequest.WindowOperation.Event]
            CLOSE: _ClassVar[AccumulatorRequest.WindowOperation.Event]
            APPEND: _ClassVar[AccumulatorRequest.WindowOperation.Event]
        OPEN: AccumulatorRequest.WindowOperation.Event
        CLOSE: AccumulatorRequest.WindowOperation.Event
        APPEND: AccumulatorRequest.WindowOperation.Event
        EVENT_FIELD_NUMBER: _ClassVar[int]
        KEYEDWINDOW_FIELD_NUMBER: _ClassVar[int]
        event: AccumulatorRequest.WindowOperation.Event
        keyedWindow: KeyedWindow
        def __init__(self, event: _Optional[_Union[AccumulatorRequest.WindowOperation.Event, str]] = ..., keyedWindow: _Optional[_Union[KeyedWindow, _Mapping]] = ...) -> None: ...
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    payload: Payload
    operation: AccumulatorRequest.WindowOperation
    def __init__(self, payload: _Optional[_Union[Payload, _Mapping]] = ..., operation: _Optional[_Union[AccumulatorRequest.WindowOperation, _Mapping]] = ...) -> None: ...

class KeyedWindow(_message.Message):
    __slots__ = ("start", "end", "slot", "keys")
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    SLOT_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    start: _timestamp_pb2.Timestamp
    end: _timestamp_pb2.Timestamp
    slot: str
    keys: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, start: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., end: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., slot: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ...) -> None: ...

class AccumulatorResponse(_message.Message):
    __slots__ = ("payload", "window", "tags", "EOF")
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    WINDOW_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    EOF_FIELD_NUMBER: _ClassVar[int]
    payload: Payload
    window: KeyedWindow
    tags: _containers.RepeatedScalarFieldContainer[str]
    EOF: bool
    def __init__(self, payload: _Optional[_Union[Payload, _Mapping]] = ..., window: _Optional[_Union[KeyedWindow, _Mapping]] = ..., tags: _Optional[_Iterable[str]] = ..., EOF: bool = ...) -> None: ...

class ReadyResponse(_message.Message):
    __slots__ = ("ready",)
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: bool = ...) -> None: ...
