from dataclasses import dataclass
from datetime import datetime
from functools import partialmethod
from typing import TypeVar, Type, List

DROP = b"U+005C__DROP__"
ALL = b"U+005C__ALL__"


M = TypeVar("M", bound="Message")
Ms = TypeVar("Ms", bound="Messages")


@dataclass(frozen=True)
class Message:
    """
    Basic datatype for data passing to the next vertex/vertices.

    Args:
        key: string key for vertex;
             special values are ALL (send to all), DROP (drop message)
        value: data in bytes
    """

    key: str = ""
    value: bytes = b""

    @classmethod
    def to_vtx(cls: Type[M], key: str, value: bytes) -> M:
        """
        Returns a Message object to send value to a vertex.
        """
        return cls(key, value)

    to_all = partialmethod(to_vtx, ALL)
    to_drop = partialmethod(to_vtx, DROP, b"")


class Messages:
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
    def as_forward_all(cls: Type[Ms], value: bytes) -> Ms:
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


class Datum:
    """
    Class to define the important information for the event.
    Args:
        value: the payload of the event.
        event_time: the event time of the event.
        watermark: the watermark of the event.
    >>> # Example usage
    >>> from pynumaflow.function import Datum
    >>> from datetime import datetime, timezone
    >>> payload = bytes("test_mock_message", encoding="utf-8")
    >>> t1 = datetime.fromtimestamp(1662998400, timezone.utc)
    >>> t2 = datetime.fromtimestamp(1662998460, timezone.utc)
    >>> d = Datum(value=payload, event_time=t1, watermark=t2)
    """

    def __init__(self, value: bytes, event_time: datetime, watermark: datetime):
        self._value = value or b""
        if not isinstance(event_time, datetime):
            raise TypeError(f"Wrong data type: {type(event_time)} for Datum.event_time")
        self._event_time = event_time
        if not isinstance(watermark, datetime):
            raise TypeError(f"Wrong data type: {type(watermark)} for Datum.watermark")
        self._watermark = watermark

    def __str__(self):
        value_string = self._value.decode("utf-8")
        return (
            f"value: {value_string}, "
            f"event_time: {str(self._event_time)}, "
            f"watermark: {str(self._watermark)}"
        )

    def __repr__(self):
        return str(self)

    @property
    def value(self):
        """Returns the value of the event."""
        return self._value

    @property
    def event_time(self):
        """Returns the event time of the event."""
        return self._event_time

    @property
    def watermark(self):
        """Returns the watermark of the event."""
        return self._watermark


class IntervalWindow:
    """Defines the start and end of the interval window for the event."""

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
