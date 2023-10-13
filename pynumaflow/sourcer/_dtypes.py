from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Callable
from collections.abc import AsyncIterable


@dataclass(init=False)
class Offset:
    """
    Args:
        offset: the offset of the datum.
        partition_id: partition_id indicates which partition of the source the datum belongs to.
    """

    __slots__ = ("_offset", "_partition_id")

    _offset: bytes
    _partition_id: str

    def __init__(self, offset: bytes, partition_id: str):
        self._offset = offset
        self._partition_id = partition_id

    @property
    def as_dict(self):
        return {"offset": self._offset, "partition_id": self._partition_id}

    @property
    def offset(self) -> bytes:
        return self._offset

    @property
    def partition_id(self) -> str:
        return self._partition_id


@dataclass(init=False)
class Message:
    """
    Basic datatype for data passing to the next vertex/vertices.

    Args:
        payload: data in bytes
        offset: the offset of the datum.
        event_time: event time of the message, usually extracted from the payload.
        keys: []string keys for vertex (optional)
    """

    __slots__ = ("_payload", "_offset", "_event_time", "_keys")

    _payload: bytes
    _offset: Offset
    _event_time: datetime
    _keys: list[str]

    def __init__(
        self, payload: bytes, offset: Offset, event_time: datetime, keys: list[str] = None
    ):
        """
        Creates a Message object to send value to a vertex.
        """
        self._payload = payload
        self._offset = offset
        self._event_time = event_time
        self._keys = keys or []

    @property
    def payload(self) -> bytes:
        return self._payload

    @property
    def keys(self) -> list[str]:
        return self._keys

    @property
    def offset(self) -> Offset:
        return self._offset

    @property
    def event_time(self) -> datetime:
        return self._event_time


@dataclass(init=False)
class ReadRequest:
    """
    Class to define the request for reading datum stream from user defined source.
    Args:
        num_records: the number of records to read.
        timeout_in_ms: the request timeout in milliseconds.
    >>> # Example usage
    >>> from pynumaflow.sourcer import ReadRequest
    >>> read_request = ReadRequest(num_records=10, timeout_in_ms=1000)
    """

    __slots__ = ("_num_records", "_timeout_in_ms")

    _num_records: int
    _timeout_in_ms: int

    def __init__(
        self,
        num_records: int,
        timeout_in_ms: int,
    ):
        if not isinstance(num_records, int):
            raise TypeError(f"Wrong data type: {type(num_records)} for ReadRequest.num_records")
        self._num_records = num_records
        if not isinstance(timeout_in_ms, int):
            raise TypeError(f"Wrong data type: {type(timeout_in_ms)} for ReadRequest.timeout_in_ms")
        self._timeout_in_ms = timeout_in_ms

    @property
    def num_records(self) -> int:
        """Returns the num_records of the request"""
        return self._num_records

    @property
    def timeout_in_ms(self) -> int:
        """Returns the timeout_in_ms of the request."""
        return self._timeout_in_ms


@dataclass(init=False)
class AckRequest:
    """
    Class for defining the request for acknowledging datum.
    It takes a list of offsets that need to be acknowledged.
    Args:
        offsets: the offsets to be acknowledged.
    >>> # Example usage
    >>> from pynumaflow.sourcer import AckRequest, Offset
    >>> offset = Offset(offset=b"123", partition_id="0")
    >>> ack_request = AckRequest(offsets=[offset, offset])
    """

    __slots__ = ("_offsets",)
    _offsets: list[Offset]

    def __init__(self, offsets: list[Offset]):
        self._offsets = offsets

    @property
    def offset(self) -> list[Offset]:
        """Returns the offsets to be acknowledged."""
        return self._offsets


@dataclass(init=False)
class PendingResponse:
    """
    PendingResponse is the response for the pending request.
    It indicates the number of pending records at the user defined source.
    A negative count indicates that the pending information is not available.
    Args:
        count: the number of pending records.
    """

    __slots__ = ("_count",)
    _count: int

    def __init__(self, count: int):
        if not isinstance(count, int):
            raise TypeError(f"Wrong data type: {type(count)} for Pending.count")
        self._count = count

    @property
    def count(self) -> int:
        """Returns the count of pending records"""
        return self._count


SourceReadCallable = Callable[[ReadRequest], Iterable[Message]]
AsyncSourceReadCallable = Callable[[ReadRequest], AsyncIterable[Message]]
SourceAckCallable = Callable[[AckRequest], None]
