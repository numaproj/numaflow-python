from abc import ABCMeta, abstractmethod
from asyncio import Task
from collections.abc import Iterator, Sequence, Awaitable
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from typing import TypeVar, Callable, Union, Optional
from collections.abc import AsyncIterable
from warnings import warn

from pynumaflow.shared.asynciter import NonBlockingIterator
from pynumaflow._constants import DROP

M = TypeVar("M", bound="Message")
Ms = TypeVar("Ms", bound="Messages")


class WindowOperation(IntEnum):
    """
    Enumerate the type of Window operation received.
    The operation can be one of the following:
    - OPEN: A new window is opened.
    - CLOSE: The window is closed.
    - APPEND: The window is appended with new data.
    """

    OPEN = (0,)
    CLOSE = (1,)
    APPEND = (4,)


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
        headers: the headers of the event.
    >>> # Example usage
    >>> from pynumaflow.reducer import Datum
    >>> from datetime import datetime, timezone
    >>> payload = bytes("test_mock_message", encoding="utf-8")
    >>> t1 = datetime.fromtimestamp(1662998400, timezone.utc)
    >>> t2 = datetime.fromtimestamp(1662998460, timezone.utc)
    >>> msg_headers = {"key1": "value1", "key2": "value2"}
    >>> d = Datum(
    ...       keys=["test_key"],
    ...       value=payload,
    ...       event_time=t1,
    ...       watermark=t2,
    ...       headers=msg_headers
    ...    )
    """

    __slots__ = ("_keys", "_value", "_event_time", "_watermark", "_headers")

    _keys: list[str]
    _value: bytes
    _event_time: datetime
    _watermark: datetime
    _headers: dict[str, str]

    def __init__(
        self,
        keys: list[str],
        value: bytes,
        event_time: datetime,
        watermark: datetime,
        headers: Optional[dict[str, str]] = None,
    ):
        self._keys = keys or list()
        self._value = value or b""
        if not isinstance(event_time, datetime):
            raise TypeError(f"Wrong data type: {type(event_time)} for Datum.event_time")
        self._event_time = event_time
        if not isinstance(watermark, datetime):
            raise TypeError(f"Wrong data type: {type(watermark)} for Datum.watermark")
        self._watermark = watermark
        self._headers = headers or {}

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

    @property
    def headers(self) -> dict[str, str]:
        """Returns the headers of the event."""
        return self._headers.copy()


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
class ReduceWindow:
    """
    Defines the window for a reduce operation which includes the
    interval window along with the slot.
    """

    __slots__ = ("_window", "_slot")

    _window: IntervalWindow
    _slot: str

    def __init__(self, start: datetime, end: datetime, slot: str = ""):
        self._window = IntervalWindow(start=start, end=end)
        self._slot = slot

    @property
    def start(self):
        """Returns the start timestamp of the interval window."""
        return self._window.start

    @property
    def end(self):
        """Returns the end timestamp of the interval window."""
        return self._window.end

    @property
    def slot(self):
        """Returns the slot from the window"""
        return self._slot

    @property
    def window(self):
        """Return the interval window"""
        return self._window


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
    """
    Defines the object to hold the result of reduce computation.
    It contains the following
    1. Future: The async awaitable result of computation
    2. Iterator: The handle to the input queue
    3. Key: The keys of the partition
    4. Window: The window of the reduce operation
    """

    __slots__ = ("_future", "_iterator", "_key", "_window")

    _future: Task
    _iterator: NonBlockingIterator
    _key: list[str]
    _window: ReduceWindow

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

    @property
    def window(self) -> ReduceWindow:
        """"""
        return self._window


@dataclass
class ReduceRequest:
    """Defines the object to hold a request for the reduce operation."""

    __slots__ = ("_operation", "_windows", "_payload")

    _operation: WindowOperation
    _windows: list[ReduceWindow]
    _payload: Datum

    def __init__(self, operation: WindowOperation, windows: list[ReduceWindow], payload: Datum):
        self._operation = operation
        self._windows = windows
        self._payload = payload

    @property
    def operation(self) -> WindowOperation:
        """
        Returns the operation of the reduce request.
        The operation can be one of the following:
        - OPEN: A new window is opened.
        - CLOSE: The window is closed.
        - APPEND: The window is appended with new data.
        """
        return self._operation

    @property
    def windows(self) -> list[ReduceWindow]:
        """
        Returns the windows of the reduce request.
        """
        return self._windows

    @property
    def payload(self) -> Datum:
        """
        Returns the payload of the reduce request.
        """
        return self._payload


ReduceAsyncCallable = Callable[[list[str], AsyncIterable[Datum], Metadata], Awaitable[Messages]]


class Reducer(metaclass=ABCMeta):
    """
    Provides an interface to write a Reducer
    which will be exposed over a gRPC server.
    """

    def __call__(self, *args, **kwargs):
        """
        Allow to call handler function directly if class instance is sent
        as the reducer_instance.
        """
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(
        self, keys: list[str], datums: AsyncIterable[Datum], md: Metadata
    ) -> Messages:
        """
        Implement this handler function which implements the ReduceCallable interface.
        """
        pass


class _ReduceBuilderClass:
    """
    Class to build a Reducer class instance.
    Used Internally

    Args:
        reducer_class: the reducer class to be used for Reduce UDF
        args: the arguments to be passed to the reducer class
        kwargs: the keyword arguments to be passed to the reducer class
    """

    def __init__(self, reducer_class: type[Reducer], args: tuple, kwargs: dict):
        self._reducer_class: type[Reducer] = reducer_class
        self._args = args
        self._kwargs = kwargs

    def create(self) -> Reducer:
        """
        Create a new Reducer instance.
        """
        return self._reducer_class(*self._args, **self._kwargs)


# ReduceCallable is a callable which can be used as a handler for the Reduce UDF.
ReduceCallable = Union[ReduceAsyncCallable, type[Reducer]]
