from asyncio import Task
from collections.abc import Iterator, Sequence, Awaitable
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar, Callable
from collections.abc import AsyncIterable
from warnings import warn

from pynumaflow.reducer.asynciter import NonBlockingIterator
from pynumaflow._constants import DROP

M = TypeVar("M", bound="Message")
Ms = TypeVar("Ms", bound="Messages")


@dataclass(init=False)
class Message:
    """
    Basic datatype for data passing to the next vertex/vertices.

    Args:
        value: data in bytes
        keys: []string keys for vertex (optional)
        tags: []string tags for conditional forwarding (optional)
    """

    __slots__ = ("_value", "_keys", "_tags")

    _value: bytes
    _keys: list[str]
    _tags: list[str]

    def __init__(self, value: bytes, keys: list[str] = None, tags: list[str] = None):
        """
        Creates a Message object to send value to a vertex.
        """
        self._keys = keys or []
        self._tags = tags or []
        self._value = value or b""

    # returns the Message Object which will be dropped
    @classmethod
    def to_drop(cls: type[M]) -> M:
        return cls(b"", None, [DROP])

    @property
    def value(self) -> bytes:
        return self._value

    @property
    def keys(self) -> list[str]:
        return self._keys

    @property
    def tags(self) -> list[str]:
        return self._tags


class Messages(Sequence[M]):
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
        keys: the keys of the event.
        value: the payload of the event.
        event_time: the event time of the event.
        watermark: the watermark of the event.
    >>> # Example usage
    >>> from pynumaflow.reducer import Datum
    >>> from datetime import datetime, timezone
    >>> payload = bytes("test_mock_message", encoding="utf-8")
    >>> t1 = datetime.fromtimestamp(1662998400, timezone.utc)
    >>> t2 = datetime.fromtimestamp(1662998460, timezone.utc)
    >>> d = Datum(
    ...       keys=["test_key"],
    ...       value=payload,
    ...       event_time=t1,
    ...       watermark=t2,
    ...    )
    """

    __slots__ = ("_keys", "_value", "_event_time", "_watermark")

    _keys: list[str]
    _value: bytes
    _event_time: datetime
    _watermark: datetime

    def __init__(
        self,
        keys: list[str],
        value: bytes,
        event_time: datetime,
        watermark: datetime,
    ):
        self._keys = keys or list()
        self._value = value or b""
        if not isinstance(event_time, datetime):
            raise TypeError(f"Wrong data type: {type(event_time)} for Datum.event_time")
        self._event_time = event_time
        if not isinstance(watermark, datetime):
            raise TypeError(f"Wrong data type: {type(watermark)} for Datum.watermark")
        self._watermark = watermark

    def keys(self) -> list[str]:
        """Returns the keys of the event"""
        return self._keys

    @property
    def value(self) -> bytes:
        """Returns the value of the event."""
        return self._value

    @property
    def event_time(self) -> datetime:
        """Returns the event time of the event."""
        return self._event_time

    @property
    def watermark(self) -> datetime:
        """Returns the watermark of the event."""
        return self._watermark


@dataclass(init=False)
class IntervalWindow:
    """Defines the start and end of the interval window for the event."""

    __slots__ = ("_start", "_end")

    _start: datetime
    _end: datetime

    def __init__(self, start: datetime, end: datetime):
        self._start = start
        self._end = end

    @property
    def start(self):
        """Returns the start point of the interval window."""
        return self._start

    @property
    def end(self):
        """Returns the end point of the interval window."""
        return self._end


@dataclass(init=False)
class Metadata:
    """Defines the metadata for the event."""

    __slots__ = ("_interval_window",)

    _interval_window: IntervalWindow

    def __init__(self, interval_window: IntervalWindow):
        self._interval_window = interval_window

    @property
    def interval_window(self):
        """Returns the interval window for the event."""
        return self._interval_window


@dataclass
class ReduceResult:
    """Defines the object to hold the result of reduce computation."""

    __slots__ = ("_future", "_iterator", "_key")

    _future: Task
    _iterator: NonBlockingIterator
    _key: list[str]

    @property
    def future(self):
        """Returns the future result of computation."""
        return self._future

    @property
    def iterator(self):
        """Returns the handle to the producer queue."""
        return self._iterator

    @property
    def keys(self) -> list[str]:
        """Returns the keys of the partition."""
        return self._key


ReduceCallable = Callable[[list[str], AsyncIterable[Datum], Metadata], Awaitable[Messages]]
