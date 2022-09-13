from datetime import datetime
from typing import TypeVar, Type, List

DROP = b"U+005C__DROP__"
ALL = b"U+005C__ALL__"


M = TypeVar("M", bound="Message")
Ms = TypeVar("Ms", bound="Messages")


class Message:
    def __init__(self, key: str, value: bytes):
        self._key = key or ""
        self._value = value or b""

    def __str__(self):
        return str({self._key: self._value})

    def __repr__(self):
        return str(self)

    @property
    def key(self) -> str:
        return self._key

    @property
    def value(self) -> bytes:
        return self._value

    @classmethod
    def to_vtx(cls: Type[M], key: str, value: bytes) -> M:
        return cls(key, value)

    @classmethod
    def to_all(cls: Type[M], value: bytes) -> M:
        return cls(ALL, value)

    @classmethod
    def to_drop(cls: Type[M]) -> M:
        return cls(DROP, b"")


class Messages:
    def __init__(self):
        self._messages = []

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
    """Defines the important information for the event."""

    def __init__(self, value: bytes, event_time: datetime, watermark: datetime):
        self._value = value or b""
        if not isinstance(event_time, datetime):
            raise TypeError(f"Wrong data type: {type(event_time)} for Datum.event_time")
        self._event_time = event_time
        if not isinstance(watermark, datetime):
            raise TypeError(f"Wrong data type: {type(watermark)} for Datum.watermark")
        self._watermark = watermark

    def __str__(self):
        return "value:%s event_time:%s watermark:%s" % (
            self._value.decode("utf-8"),
            str(self._event_time),
            str(self._watermark),
        )

    def __repr__(self):
        return str(self)

    def value(self):
        """Returns the value of the event."""
        return self._value

    def event_time(self):
        """Returns the event time of the event."""
        return self._event_time

    def watermark(self):
        """Returns the watermark of the event."""
        return self._watermark


class IntervalWindow:
    """Defines the start and end of the interval window for the event."""

    def __init__(self, start: datetime, end: datetime):
        self._start = start
        self._end = end

    def __str__(self):
        return "start:%s end:%s" % (str(self._start), str(self._end))

    def __repr__(self):
        return str(self)

    def start(self):
        """Returns the start point of the interval window."""
        return self._start

    def end(self):
        """Returns the end point of the interval window."""
        return self._end


class Metadata:
    """Defines the metadata for the event."""

    def __init__(self, interval_window: IntervalWindow):
        self._interval_window = interval_window

    def __str__(self):
        return "interval_window:%s" % self._interval_window

    def __repr__(self):
        return str(self)

    def interval_window(self):
        """Returns the interval window for the event."""
        return self._interval_window
