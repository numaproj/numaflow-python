from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from typing import (
    ClassVar as _ClassVar,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
    List,
)

DESCRIPTOR: _descriptor.FileDescriptor

class ReadyResponse(_message.Message):
    __slots__ = ["ready"]
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: _Optional[bool] = ...) -> None: ...

class EventTime(_message.Message):
    __slots__ = ["event_time"]
    EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    event_time: _timestamp_pb2.Timestamp
    def __init__(self, event_time: _Optional[_timestamp_pb2.Timestamp] = ...) -> None: ...

class Watermark(_message.Message):
    __slots__ = ["watermark"]
    WATERMARK_FIELD_NUMBER: _ClassVar[int]
    watermark: _timestamp_pb2.Timestamp
    def __init__(self, watermark: _Optional[_timestamp_pb2.Timestamp] = ...) -> None: ...

class Datum(_message.Message):
    __slots__ = ["key", "value", "event_time", "watermark", "id"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    WATERMARK_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    id: str
    event_time: _timestamp_pb2.Timestamp
    watermark: _timestamp_pb2.Timestamp
    def __init__(
        self,
        key: _Optional[str],
        value: _Optional[bytes],
        id: _Optional[str],
        event_time: _Optional[_timestamp_pb2.Timestamp] = ...,
        watermark: _Optional[_timestamp_pb2.Timestamp] = ...,
    ) -> None: ...

class DatumList(_message.Message):
    __slots__ = ["elements"]
    ELEMENTS_FIELD_NUMBER: _ClassVar[int]
    elements: List[Datum]
    def __init__(self, elements: _Optional[List[Datum]]) -> None: ...

class Response(_message.Message):
    __slots__ = ["id", "success", "err_msg"]
    ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERR_MSG_FIELD_NUMBER: _ClassVar[int]
    id: str
    success: bool
    err_msg: str
    def __init__(
        self,
        id: _Optional[str],
        success: _Optional[bool],
        err_msg: _Optional[str],
    ) -> None: ...

class ResponseList(_message.Message):
    __slots__ = ["responses"]
    RESPONSES_FIELD_NUMBER: _ClassVar[int]
    responses: List[Response]
    def __init__(self, responses: _Optional[List[Response]]) -> None: ...
