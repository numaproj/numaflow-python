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
