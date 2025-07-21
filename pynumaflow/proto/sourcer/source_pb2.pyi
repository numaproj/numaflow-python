from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import empty_pb2 as _empty_pb2
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

class Handshake(_message.Message):
    __slots__ = ("sot",)
    SOT_FIELD_NUMBER: _ClassVar[int]
    sot: bool
    def __init__(self, sot: bool = ...) -> None: ...

class ReadRequest(_message.Message):
    __slots__ = ("request", "handshake")

    class Request(_message.Message):
        __slots__ = ("num_records", "timeout_in_ms")
        NUM_RECORDS_FIELD_NUMBER: _ClassVar[int]
        TIMEOUT_IN_MS_FIELD_NUMBER: _ClassVar[int]
        num_records: int
        timeout_in_ms: int
        def __init__(
            self, num_records: _Optional[int] = ..., timeout_in_ms: _Optional[int] = ...
        ) -> None: ...
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    HANDSHAKE_FIELD_NUMBER: _ClassVar[int]
    request: ReadRequest.Request
    handshake: Handshake
    def __init__(
        self,
        request: _Optional[_Union[ReadRequest.Request, _Mapping]] = ...,
        handshake: _Optional[_Union[Handshake, _Mapping]] = ...,
    ) -> None: ...

class ReadResponse(_message.Message):
    __slots__ = ("result", "status", "handshake")

    class Result(_message.Message):
        __slots__ = ("payload", "offset", "event_time", "keys", "headers")

        class HeadersEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: str
            def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
        PAYLOAD_FIELD_NUMBER: _ClassVar[int]
        OFFSET_FIELD_NUMBER: _ClassVar[int]
        EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
        KEYS_FIELD_NUMBER: _ClassVar[int]
        HEADERS_FIELD_NUMBER: _ClassVar[int]
        payload: bytes
        offset: Offset
        event_time: _timestamp_pb2.Timestamp
        keys: _containers.RepeatedScalarFieldContainer[str]
        headers: _containers.ScalarMap[str, str]
        def __init__(
            self,
            payload: _Optional[bytes] = ...,
            offset: _Optional[_Union[Offset, _Mapping]] = ...,
            event_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...,
            keys: _Optional[_Iterable[str]] = ...,
            headers: _Optional[_Mapping[str, str]] = ...,
        ) -> None: ...

    class Status(_message.Message):
        __slots__ = ("eot", "code", "error", "msg")

        class Code(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            SUCCESS: _ClassVar[ReadResponse.Status.Code]
            FAILURE: _ClassVar[ReadResponse.Status.Code]
        SUCCESS: ReadResponse.Status.Code
        FAILURE: ReadResponse.Status.Code

        class Error(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            UNACKED: _ClassVar[ReadResponse.Status.Error]
            OTHER: _ClassVar[ReadResponse.Status.Error]
        UNACKED: ReadResponse.Status.Error
        OTHER: ReadResponse.Status.Error
        EOT_FIELD_NUMBER: _ClassVar[int]
        CODE_FIELD_NUMBER: _ClassVar[int]
        ERROR_FIELD_NUMBER: _ClassVar[int]
        MSG_FIELD_NUMBER: _ClassVar[int]
        eot: bool
        code: ReadResponse.Status.Code
        error: ReadResponse.Status.Error
        msg: str
        def __init__(
            self,
            eot: bool = ...,
            code: _Optional[_Union[ReadResponse.Status.Code, str]] = ...,
            error: _Optional[_Union[ReadResponse.Status.Error, str]] = ...,
            msg: _Optional[str] = ...,
        ) -> None: ...
    RESULT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    HANDSHAKE_FIELD_NUMBER: _ClassVar[int]
    result: ReadResponse.Result
    status: ReadResponse.Status
    handshake: Handshake
    def __init__(
        self,
        result: _Optional[_Union[ReadResponse.Result, _Mapping]] = ...,
        status: _Optional[_Union[ReadResponse.Status, _Mapping]] = ...,
        handshake: _Optional[_Union[Handshake, _Mapping]] = ...,
    ) -> None: ...

class AckRequest(_message.Message):
    __slots__ = ("request", "handshake")

    class Request(_message.Message):
        __slots__ = ("offsets",)
        OFFSETS_FIELD_NUMBER: _ClassVar[int]
        offsets: _containers.RepeatedCompositeFieldContainer[Offset]
        def __init__(
            self, offsets: _Optional[_Iterable[_Union[Offset, _Mapping]]] = ...
        ) -> None: ...
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    HANDSHAKE_FIELD_NUMBER: _ClassVar[int]
    request: AckRequest.Request
    handshake: Handshake
    def __init__(
        self,
        request: _Optional[_Union[AckRequest.Request, _Mapping]] = ...,
        handshake: _Optional[_Union[Handshake, _Mapping]] = ...,
    ) -> None: ...

class AckResponse(_message.Message):
    __slots__ = ("result", "handshake")

    class Result(_message.Message):
        __slots__ = ("success",)
        SUCCESS_FIELD_NUMBER: _ClassVar[int]
        success: _empty_pb2.Empty
        def __init__(
            self, success: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ...
        ) -> None: ...
    RESULT_FIELD_NUMBER: _ClassVar[int]
    HANDSHAKE_FIELD_NUMBER: _ClassVar[int]
    result: AckResponse.Result
    handshake: Handshake
    def __init__(
        self,
        result: _Optional[_Union[AckResponse.Result, _Mapping]] = ...,
        handshake: _Optional[_Union[Handshake, _Mapping]] = ...,
    ) -> None: ...

class ReadyResponse(_message.Message):
    __slots__ = ("ready",)
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: bool = ...) -> None: ...

class PendingResponse(_message.Message):
    __slots__ = ("result",)

    class Result(_message.Message):
        __slots__ = ("count",)
        COUNT_FIELD_NUMBER: _ClassVar[int]
        count: int
        def __init__(self, count: _Optional[int] = ...) -> None: ...
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: PendingResponse.Result
    def __init__(
        self, result: _Optional[_Union[PendingResponse.Result, _Mapping]] = ...
    ) -> None: ...

class PartitionsResponse(_message.Message):
    __slots__ = ("result",)

    class Result(_message.Message):
        __slots__ = ("partitions",)
        PARTITIONS_FIELD_NUMBER: _ClassVar[int]
        partitions: _containers.RepeatedScalarFieldContainer[int]
        def __init__(self, partitions: _Optional[_Iterable[int]] = ...) -> None: ...
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: PartitionsResponse.Result
    def __init__(
        self, result: _Optional[_Union[PartitionsResponse.Result, _Mapping]] = ...
    ) -> None: ...

class Offset(_message.Message):
    __slots__ = ("offset", "partition_id")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    offset: bytes
    partition_id: int
    def __init__(
        self, offset: _Optional[bytes] = ..., partition_id: _Optional[int] = ...
    ) -> None: ...
