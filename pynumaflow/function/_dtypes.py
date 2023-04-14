from asyncio import Task
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar, List, Type

from pynumaflow.function.asynciter import NonBlockingIterator

DROP = "U+005C__DROP__"

M = TypeVar("M", bound="Message")
Ms = TypeVar("Ms", bound="Messages")
MT = TypeVar("MT", bound="MessageT")
MTs = TypeVar("MTs", bound="MessageTs")


@dataclass
class Message:
    """
    Basic datatype for data passing to the next vertex/vertices.

    Args:
        _value: data in bytes
         _keys: []string keys for vertex (optional)
         _tags: []string tags for conditional forwarding (optional)
    """

    _keys: List[str]
    _tags: List[str]
    _value: bytes = b""

    def __init__(self, value: bytes, keys=None, tags=None):
        """
        Creates a Message object to send value to a vertex.
        """
        self._keys = keys or []
        self._tags = tags or []
        self._value = value or b""

    # returns the Message Object which will be dropped
    @classmethod
    def to_drop(cls: Type[M]) -> M:
        return cls(b"", None, [DROP])

    @property
    def value(self) -> bytes:
        return self._value

    @property
    def keys(self) -> List[str]:
        return self._keys

    @property
    def tags(self) -> List[str]:
        return self._tags


class Messages:
    """
    Class to define a list of Message objects.

    Args:
        messages: list of Message objects.
    """

    __slots__ = ("_messages",)

    def __init__(self, *messages: M):
        self._messages = list(messages) or []

    def __str__(self):
        return str(self._messages)

    def __repr__(self):
        return str(self)

    def append(self, message: Message) -> None:
        self._messages.append(message)

    def items(self) -> List[Message]:
        return self._messages

    def dumps(self) -> str:
        return str(self)

    def loads(self) -> Ms:
        pass


@dataclass
class MessageT:
    """
    Basic datatype for data passing to the next vertex/vertices.

    Args:
        _value: data in bytes
        _event_time: event time of the message, usually extracted from the payload.
         _keys: []string keys for vertex (optional)
         _tags: []string tags for conditional forwarding (optional)
    """

    _keys: List[str]
    _tags: List[str]
    _value: bytes = b""
    # There is no year 0, so setting following as default event time.
    _event_time: datetime = datetime(1, 1, 1, 0, 0)

    def __init__(self, value: bytes, event_time: datetime, keys=None, tags=None):
        """
        Creates a MessageT object to send value to a vertex.
        """
        self._tags = tags or []
        self._keys = keys or []
        self._event_time = event_time or datetime(1, 1, 1, 0, 0)
        self._value = value or b""

    @classmethod
    def to_drop(cls: Type[MT]) -> MT:
        return cls(b"", datetime(1, 1, 1, 0, 0), None, [DROP])

    @property
    def event_time(self) -> datetime:
        return self._event_time

    @property
    def keys(self) -> List[str]:
        return self._keys

    @property
    def value(self) -> bytes:
        return self._value

    @property
    def tags(self) -> List[str]:
        return self._tags


class MessageTs:
    """
    Class to define a list of MessageT objects.

    Args:
        message_ts: list of MessageT objects.
    """

    __slots__ = ("_message_ts",)

    def __init__(self, *message_ts: MT):
        self._message_ts = list(message_ts) or []

    def __str__(self):
        return str(self._message_ts)

    def __repr__(self):
        return str(self)

    def append(self, message_t: MessageT) -> None:
        self._message_ts.append(message_t)

    def items(self) -> List[MessageT]:
        return self._message_ts

    def dumps(self) -> str:
        return str(self)

    def loads(self) -> Ms:
        pass


class DatumMetadata:
    """
    Class to define the metadata information for the event.
    Args:
        msg_id: the id of the event.
        num_delivered: the number the event has been delivered.
    >>> # Example usage
    >>> from pynumaflow.function import DatumMetadata
    >>> msg_id = "id"
    >>> num_delivered = 1
    >>> d = Datum(id=msg_id, num_delivered=num_delivered)
    """

    def __init__(
        self,
        msg_id: str,
        num_delivered: int,
    ):
        self._id = msg_id or ""
        self._num_delivered = num_delivered or 0

    def __str__(self):
        return f"id: {self._id}, " f"num_delivered: {str(self._num_delivered)}"

    def __repr__(self):
        return str(self)

    @property
    def id(self) -> str:
        """Returns the id of the event."""
        return self._id

    @property
    def num_delivered(self):
        """Returns the number the event has been delivered."""
        return self._num_delivered


class Datum:
    """
    Class to define the important information for the event.
    Args:
        keys: the keys of the event.
        value: the payload of the event.
        event_time: the event time of the event.
        watermark: the watermark of the event.
        metadata: the metadata of the event.
    >>> # Example usage
    >>> from pynumaflow.function import Datum
    >>> from datetime import datetime, timezone
    >>> keys = ["test_key"]
    >>> payload = bytes("test_mock_message", encoding="utf-8")
    >>> t1 = datetime.fromtimestamp(1662998400, timezone.utc)
    >>> t2 = datetime.fromtimestamp(1662998460, timezone.utc)
    >>> metadata = DatumMetadata(msg_id="test_id", num_delivered=1)
    >>> d = Datum(keys=keys, value=payload, event_time=t1, watermark=t2, metadata=metadata
    """

    __slots__ = ("_keys", "_value", "_event_time", "_watermark", "_metadata")

    def __init__(
        self,
        keys: List[str],
        value: bytes,
        event_time: datetime,
        watermark: datetime,
        metadata: DatumMetadata,
    ):
        self._keys = keys or list()
        self._value = value or b""
        if not isinstance(event_time, datetime):
            raise TypeError(f"Wrong data type: {type(event_time)} for Datum.event_time")
        self._event_time = event_time
        if not isinstance(watermark, datetime):
            raise TypeError(f"Wrong data type: {type(watermark)} for Datum.watermark")
        self._watermark = watermark
        self._metadata = metadata

    def __str__(self):
        value_string = self._value.decode("utf-8")
        return (
            f"keys: {self._keys}, "
            f"value: {value_string}, "
            f"event_time: {str(self._event_time)}, "
            f"watermark: {str(self._watermark)}, "
            f"metadata: {str(self._metadata)}"
        )

    def __repr__(self):
        return str(self)

    def keys(self) -> List[str]:
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
    def metadata(self) -> DatumMetadata:
        """Returns the metadata of the event."""
        return self._metadata


class IntervalWindow:
    """Defines the start and end of the interval window for the event."""

    __slots__ = ("_start", "_end")

    def __init__(self, start: datetime, end: datetime):
        self._start = start
        self._end = end

    def __str__(self):
        return f"start: {self._start}, end: {self._end}"

    def __repr__(self):
        return str(self)

    @property
    def start(self):
        """Returns the start point of the interval window."""
        return self._start

    @property
    def end(self):
        """Returns the end point of the interval window."""
        return self._end


class Metadata:
    """Defines the metadata for the event."""

    __slots__ = ("_interval_window",)

    def __init__(self, interval_window: IntervalWindow):
        self._interval_window = interval_window

    def __str__(self):
        return f"interval_window: {self._interval_window}"

    def __repr__(self):
        return str(self)

    @property
    def interval_window(self):
        """Returns the interval window for the event."""
        return self._interval_window


class ReduceResult:
    """Defines the object to hold the result of reduce computation."""

    __slots__ = ("_future", "_iterator", "_key")

    def __init__(self, future: Task, iterator: NonBlockingIterator, keys: List[str]):
        self._future = future
        self._iterator = iterator
        self._key = keys

    @property
    def future(self):
        """Returns the future result of computation."""
        return self._future

    @property
    def iterator(self):
        """Returns the handle to the producer queue."""
        return self._iterator

    @property
    def keys(self) -> List[str]:
        """Returns the keys of the partition."""
        return self._key
