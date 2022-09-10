from datetime import time
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
        return self.__str__()

    def loads(self) -> Ms:
        pass


class Datum:

    def __init__(self, value: bytes, event_time: time, water_mark: time):
        self._value = value or b""
        self._event_time = event_time
        self._water_mark = water_mark

    def __str__(self):
        return str({self._value, self._event_time, self._water_mark})

    def __repr__(self):
        return str(self)

    def value(self):
        return self._value

    def event_time(self):
        return self._event_time

    def water_mark(self):
        return self._water_mark


class IntervalWindow:

    def __init__(self, start: time, end: time):
        self._start = start
        self._end = end

    def __str__(self):
        return str([self._start, str(self._end)])

    def __repr__(self):
        return str(self)

    def start(self):
        return self._start

    def end(self):
        return self._end


class Metadata:
    def __init__(self, interval_window: IntervalWindow):
        self._interval_window = interval_window

    def __str__(self):
        return str({self._interval_window})

    def __repr__(self):
        return str(self)

    def interval_window(self):
        return self._interval_window
