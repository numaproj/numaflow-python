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
