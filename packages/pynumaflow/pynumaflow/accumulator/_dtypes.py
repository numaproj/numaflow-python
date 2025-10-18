from abc import ABCMeta, abstractmethod
from asyncio import Task
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from typing import TypeVar, Callable, Union, Optional
from collections.abc import AsyncIterable

from pynumaflow.shared.asynciter import NonBlockingIterator
from pynumaflow._constants import DROP

M = TypeVar("M", bound="Message")


class WindowOperation(IntEnum):
    """
    Enumerate the type of Window operation received.
    """

    OPEN = (0,)
    CLOSE = (1,)
    APPEND = (2,)


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
    >>> from pynumaflow.accumulator import Datum
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

    __slots__ = ("_keys", "_value", "_event_time", "_watermark", "_headers", "_id")

    _keys: list[str]
    _value: bytes
    _event_time: datetime
    _watermark: datetime
    _headers: dict[str, str]
    _id: str

    def __init__(
        self,
        keys: list[str],
        value: bytes,
        event_time: datetime,
        watermark: datetime,
        id_: str,
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
        self._id = id_

    @property
    def keys(self) -> list[str]:
        """Returns the keys of the event.

        Returns:
            list[str]: A list of string keys associated with this event.
        """
        return self._keys

    @property
    def value(self) -> bytes:
        """Returns the value of the event.

        Returns:
            bytes: The payload data of the event as bytes.
        """
        return self._value

    @property
    def event_time(self) -> datetime:
        """Returns the event time of the event.

        Returns:
            datetime: The timestamp when the event occurred.
        """
        return self._event_time

    @property
    def watermark(self) -> datetime:
        """Returns the watermark of the event.

        Returns:
            datetime: The watermark timestamp indicating the progress of event time.
        """
        return self._watermark

    @property
    def headers(self) -> dict[str, str]:
        """Returns the headers of the event.

        Returns:
            dict[str, str]: A dictionary containing header key-value pairs for this event.
        """
        return self._headers.copy()

    @property
    def id(self) -> str:
        """Returns the id of the event.

        Returns:
            str: The unique identifier for this event.
        """
        return self._id


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
    def start(self) -> datetime:
        """Returns the start point of the interval window.

        Returns:
            datetime: The start timestamp of the interval window.
        """
        return self._start

    @property
    def end(self) -> datetime:
        """Returns the end point of the interval window.

        Returns:
            datetime: The end timestamp of the interval window.
        """
        return self._end


@dataclass(init=False)
class KeyedWindow:
    """
    Defines the window for a accumulator operation which includes the
    interval window along with the slot.
    """

    __slots__ = ("_window", "_slot", "_keys")

    _window: IntervalWindow
    _slot: str
    _keys: list[str]

    def __init__(self, start: datetime, end: datetime, slot: str = "", keys: list[str] = []):
        self._window = IntervalWindow(start=start, end=end)
        self._slot = slot
        self._keys = keys

    @property
    def start(self) -> datetime:
        """Returns the start point of the interval window.

        Returns:
            datetime: The start timestamp of the interval window.
        """
        return self._window.start

    @property
    def end(self) -> datetime:
        """Returns the end point of the interval window.

        Returns:
            datetime: The end timestamp of the interval window.
        """
        return self._window.end

    @property
    def slot(self) -> str:
        """Returns the slot from the window.

        Returns:
            str: The slot identifier for this window.
        """
        return self._slot

    @property
    def window(self) -> IntervalWindow:
        """Returns the interval window.

        Returns:
            IntervalWindow: The underlying interval window object.
        """
        return self._window

    @property
    def keys(self) -> list[str]:
        """Returns the keys for window.

        Returns:
            list[str]: A list of keys associated with this window.
        """
        return self._keys


@dataclass
class AccumulatorResult:
    """Defines the object to hold the result of accumulator computation."""

    __slots__ = (
        "_future",
        "_iterator",
        "_key",
        "_result_queue",
        "_consumer_future",
        "_latest_watermark",
    )

    _future: Task
    _iterator: NonBlockingIterator
    _key: list[str]
    _result_queue: NonBlockingIterator
    _consumer_future: Task
    _latest_watermark: datetime

    @property
    def future(self) -> Task:
        """Returns the future result of computation.

        Returns:
            Task: The asyncio Task representing the computation future.
        """
        return self._future

    @property
    def iterator(self) -> NonBlockingIterator:
        """Returns the handle to the producer queue.

        Returns:
            NonBlockingIterator: The iterator for producing data to the queue.
        """
        return self._iterator

    @property
    def keys(self) -> list[str]:
        """Returns the keys of the partition.

        Returns:
            list[str]: The keys associated with this partition.
        """
        return self._key

    @property
    def result_queue(self) -> NonBlockingIterator:
        """Returns the async queue used to write the output for the tasks.

        Returns:
            NonBlockingIterator: The queue for writing task output.
        """
        return self._result_queue

    @property
    def consumer_future(self) -> Task:
        """Returns the async consumer task for the result queue.

        Returns:
            Task: The asyncio Task for consuming from the result queue.
        """
        return self._consumer_future

    @property
    def latest_watermark(self) -> datetime:
        """Returns the latest watermark for task.

        Returns:
            datetime: The latest watermark timestamp for this task.
        """
        return self._latest_watermark

    def update_watermark(self, new_watermark: datetime):
        """Updates the latest watermark value.

        Args:
            new_watermark (datetime): The new watermark timestamp to set.

        Raises:
            TypeError: If new_watermark is not a datetime object.
        """
        if not isinstance(new_watermark, datetime):
            raise TypeError("new_watermark must be a datetime object")
        self._latest_watermark = new_watermark


@dataclass
class AccumulatorRequest:
    """Defines the object to hold a request for the accumulator operation."""

    __slots__ = ("_operation", "_keyed_window", "_payload")

    _operation: WindowOperation
    _keyed_window: KeyedWindow
    _payload: Datum

    def __init__(self, operation: WindowOperation, keyed_window: KeyedWindow, payload: Datum):
        self._operation = operation
        self._keyed_window = keyed_window
        self._payload = payload

    @property
    def operation(self) -> WindowOperation:
        """Returns the operation type.

        Returns:
            WindowOperation: The type of window operation (OPEN, CLOSE, or APPEND).
        """
        return self._operation

    @property
    def keyed_window(self) -> KeyedWindow:
        """Returns the keyed window.

        Returns:
            KeyedWindow: The keyed window associated with this request.
        """
        return self._keyed_window

    @property
    def payload(self) -> Datum:
        """Returns the payload of the window.

        Returns:
            Datum: The data payload for this accumulator request.
        """
        return self._payload


@dataclass(init=False)
class Message:
    """
    Basic datatype for data passing to the next vertex/vertices.

    Args:
        value: data in bytes
        keys: []string keys for vertex (optional)
        tags: []string tags for conditional forwarding (optional)
        watermark: watermark for this message (optional)
        event_time: event time for this message (optional)
        headers: headers for this message (optional)
        id: message id (optional)
    """

    __slots__ = ("_value", "_keys", "_tags", "_watermark", "_event_time", "_headers", "_id")

    _value: bytes
    _keys: list[str]
    _tags: list[str]
    _watermark: datetime
    _event_time: datetime
    _headers: dict[str, str]
    _id: str

    def __init__(
        self,
        value: bytes,
        keys: list[str] = None,
        tags: list[str] = None,
        watermark: datetime = None,
        event_time: datetime = None,
        headers: dict[str, str] = None,
        id: str = None,
    ):
        """
        Creates a Message object to send value to a vertex.
        """
        self._keys = keys or []
        self._tags = tags or []
        self._value = value or b""
        self._watermark = watermark
        self._event_time = event_time
        self._headers = headers or {}
        self._id = id or ""

    @classmethod
    def to_drop(cls: type[M]) -> M:
        """Creates a Message instance that indicates the message should be dropped.

        Returns:
            M: A Message instance with empty value and DROP tag indicating
               the message should be dropped.
        """
        return cls(b"", None, [DROP])

    @property
    def value(self) -> bytes:
        """Returns the message payload value.

        Returns:
            bytes: The message payload data as bytes.
        """
        return self._value

    @property
    def keys(self) -> list[str]:
        """Returns the message keys.

        Returns:
            list[str]: A list of string keys associated with this message.
        """
        return self._keys

    @property
    def tags(self) -> list[str]:
        """Returns the message tags for conditional forwarding.

        Returns:
            list[str]: A list of string tags used for conditional forwarding.
        """
        return self._tags

    @property
    def watermark(self) -> datetime:
        """Returns the watermark timestamp for this message.

        Returns:
            datetime: The watermark timestamp, or None if not set.
        """
        return self._watermark

    @property
    def event_time(self) -> datetime:
        """Returns the event time for this message.

        Returns:
            datetime: The event time timestamp, or None if not set.
        """
        return self._event_time

    @property
    def headers(self) -> dict[str, str]:
        """Returns the message headers.

        Returns:
            dict[str, str]: A dictionary containing header key-value pairs for this message.
        """
        return self._headers.copy()

    @property
    def id(self) -> str:
        """Returns the message ID.

        Returns:
            str: The unique identifier for this message.
        """
        return self._id

    @classmethod
    def from_datum(cls, datum: Datum):
        """Create a Message instance from a Datum object.

        Args:
            datum: The Datum object to convert

        Returns:
            Message: A new Message instance with data from the datum
        """
        return cls(
            value=datum.value,
            keys=datum.keys,
            watermark=datum.watermark,
            event_time=datum.event_time,
            headers=datum.headers,
            id=datum.id,
        )


AccumulatorAsyncCallable = Callable[[list[str], AsyncIterable[Datum], NonBlockingIterator], None]


class Accumulator(metaclass=ABCMeta):
    """
    Accumulate can read unordered from the input stream and emit the ordered
    data to the output stream. Once the watermark (WM) of the output stream progresses,
    the data in WAL until that WM will be garbage collected.
    NOTE: A message can be silently dropped if need be,
    and it will be cleared from the WAL when the WM progresses.
    """

    def __call__(self, *args, **kwargs):
        """
        Allow to call handler function directly if class instance is sent
        as the accumulator_instance.
        """
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(
        self,
        datums: AsyncIterable[Datum],
        output: NonBlockingIterator,
    ):
        """
        Implement this handler function which implements the AccumulatorStreamCallable interface.
        """
        pass


class _AccumulatorBuilderClass:
    """
    Class to build an Accumulator class instance.
    Used Internally

    Args:
        accumulator_class: the Accumulator class to be used for Accumulator UDF
        args: the arguments to be passed to the accumulator class
        kwargs: the keyword arguments to be passed to the accumulator class
    """

    def __init__(self, accumulator_class: type[Accumulator], args: tuple, kwargs: dict):
        self._accumulator_class: type[Accumulator] = accumulator_class
        self._args = args
        self._kwargs = kwargs

    def create(self) -> Accumulator:
        """
        Create a new Accumulator instance.
        """
        return self._accumulator_class(*self._args, **self._kwargs)


# AccumulatorStreamCallable is a callable which can be used as a handler for the Accumulator UDF.
AccumulatorStreamCallable = Union[AccumulatorAsyncCallable, type[Accumulator]]
