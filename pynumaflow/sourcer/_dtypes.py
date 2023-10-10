from collections.abc import Iterator, Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar, Callable
from collections.abc import AsyncIterable
from warnings import warn

from pynumaflow._constants import DROP

M = TypeVar("M", bound="Message")
Ms = TypeVar("Ms", bound="Messages")


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
    def dict(self):
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

    def dict(self):
        return {
            "payload": self._payload,
            "offset": self._offset.dict,
            "event_time": self._event_time,
            "keys": self._keys,
        }

    # returns the Message Object which will be dropped
    @classmethod
    def to_drop(cls: type[M]) -> M:
        return cls(b"", None, [DROP])

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


class Messages(Iterable[M]):
    """
    Class to define a list of Message objects.

    Args:
        messages: list of Message objects.
    """

    __slots__ = ("_messages",)

    def __init__(self, *messages: M):
        self._messages = list(messages) or []

    def __str__(self) -> str:
        return str(self._messages)

    def __repr__(self) -> str:
        return str(self)

    def __len__(self) -> int:
        return len(self._messages)

    def __iter__(self) -> Iterator[M]:
        return iter(self._messages)

    def __getitem__(self, index: int) -> M:
        if isinstance(index, slice):
            raise TypeError("Slicing is not supported for Messages")
        return self._messages[index]

    def append(self, message: Message) -> None:
        self._messages.append(message)

    def items(self) -> list[Message]:
        warn(
            "Using items is deprecated and will be removed in v0.5. "
            "Iterate or index the Messages object instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._messages


@dataclass(init=False)
class Datum:
    """
    Class to define the important information for the event.
    Args:
        num_records: the number of records to read.
        timeout_in_ms: the request timeout in milliseconds.
    >>> # Example usage
    >>> from pynumaflow.sourcer import Datum
    >>> from datetime import datetime, timezone
    >>> datum = Datum(num_records=10, timeout_in_ms=1000)
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
            raise TypeError(f"Wrong data type: {type(num_records)} for Datum.num_records")
        self._num_records = num_records
        if not isinstance(timeout_in_ms, int):
            raise TypeError(f"Wrong data type: {type(timeout_in_ms)} for Datum.timeout_in_ms")
        self._timeout_in_ms = timeout_in_ms

    @property
    def num_records(self) -> int:
        """Returns the num_records of the event"""
        return self._num_records

    @property
    def timeout_in_ms(self) -> int:
        """Returns the timeout_in_ms of the event."""
        return self._timeout_in_ms


@dataclass(init=False)
class AckRequest:
    """
    Class to define the ack information for the event.
    """

    __slots__ = "_offset"
    _offset: list[Offset]

    def __init__(self, offset: list[Offset]):
        self._offset = offset

    @property
    def offset(self) -> list[Offset]:
        """Returns the offset of the event"""
        return self._offset


@dataclass(init=False)
class PendingResponse:
    """
    Class to define the ack information for the event.
    """

    __slots__ = "_count"
    _count: int

    def __init__(self, count: int):
        if not isinstance(count, int):
            raise TypeError(f"Wrong data type: {type(count)} for Pending.count")
        self._count = count

    @property
    def count(self) -> int:
        """Returns the count of the event"""
        return self._count


SourceReadCallable = Callable[[Datum], Iterable[Message]]
AsyncSourceReadCallable = Callable[[Datum], AsyncIterable[Message]]
SourceAckCallable = Callable[[AckRequest], None]
