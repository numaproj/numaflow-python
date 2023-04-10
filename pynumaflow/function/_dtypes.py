from asyncio import Task
from dataclasses import dataclass
from datetime import datetime
from functools import partialmethod
from typing import TypeVar, Type, List, Optional

from pynumaflow.function.asynciter import NonBlockingIterator
from pynumaflow.types import NoPublicConstructor

DROP = b"U+005C__DROP__"
ALL = b"U+005C__ALL__"

M = TypeVar("M", bound="Message")
Ms = TypeVar("Ms", bound="Messages")
MT = TypeVar("MT", bound="MessageT")
MTs = TypeVar("MTs", bound="MessageTs")


@dataclass(frozen=True)
class Message(metaclass=NoPublicConstructor):
    """
    Basic datatype for data passing to the next vertex/vertices.

    Args:
        _keys: []string keys for vertex;
             special values are ALL (send to all), DROP (drop message)
        _value: data in bytes
    """

    _keys: List[str]
    _value: bytes = b""

    @classmethod
    def to_vtx(cls: Type[M], keys: List[str], value: bytes) -> M:
        """
        Returns a Message object to send value to a vertex.
        """
        return cls._create(keys, value)

    to_all = partialmethod(to_vtx, [ALL])
    to_drop = partialmethod(to_vtx, [DROP], b"")

    @property
    def value(self):
        return self._value

    @property
    def keys(self):
        return self._keys


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

    @classmethod
    def as_forward_all(cls: Type[Ms], value: Optional[bytes]) -> Ms:
        msgs = cls()
        if value:
            msgs.append(Message.to_all(value=value))
        else:
            msgs.append(Message.to_drop())
        return msgs

    def dumps(self) -> str:
        return str(self)

    def loads(self) -> Ms:
        pass


@dataclass(frozen=True)
class MessageT(metaclass=NoPublicConstructor):
    """
    Basic datatype for data passing to the next vertex/vertices.

    Args:
        _keys: []string keys for vertex;
             special values are ALL (send to all), DROP (drop message)
        _value: data in bytes
        _event_time: event time of the message, usually extracted from the payload.
    """

    _keys: List[str]
    _value: bytes = b""
    # There is no year 0, so setting following as default event time.
    _event_time: datetime = datetime(1, 1, 1, 0, 0)

    @classmethod
    def to_vtx(cls: Type[MT], keys: List[str], value: bytes, event_time: datetime) -> MT:
        """
        Returns a MessageT object to send value to a vertex.
        """
        return cls._create(keys, value, event_time)

    to_all = partialmethod(to_vtx, [ALL])
    to_drop = partialmethod(to_vtx, [DROP], b"", datetime(1, 1, 1, 0, 0))

    @property
    def event_time(self):
        return self._event_time

    @property
    def keys(self):
        return self._keys

    @property
    def value(self):
        return self._value


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

    @classmethod
    def as_forward_all(
        cls: Type[MTs], value: Optional[bytes], event_time: Optional[datetime]
    ) -> MTs:
        msg_ts = cls()
        if value and event_time:
            msg_ts.append(MessageT.to_all(value=value, event_time=event_time))
        else:
            msg_ts.append(MessageT.to_drop())
        return msg_ts

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
