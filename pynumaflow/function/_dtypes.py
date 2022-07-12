import json
from typing import TypeVar, Type, List

import msgpack

from pynumaflow._constants import APPLICATION_JSON, APPLICATION_MSG_PACK
from pynumaflow.exceptions import MarshalError
from pynumaflow.encoder import msgpack_encoding, NumaflowJSONEncoder

DROP = b"U+005C__DROP__"
ALL = b"U+005C__ALL__"


M = TypeVar("M", bound="Message")
Ms = TypeVar("Ms", bound="Messages")


class Message:
    def __init__(self, key: bytes, value: bytes):
        self._key = key or b""
        self._value = value or b""

    def __str__(self):
        return str({self._key: self._value})

    def __repr__(self):
        return str(self)

    @property
    def key(self) -> bytes:
        return self._key

    @property
    def value(self) -> bytes:
        return self._value

    @classmethod
    def to_vtx(cls: Type[M], key: bytes, value: bytes) -> M:
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
    def as_forward_all(cls: Type[Ms], json_data: str) -> Ms:
        msgs = cls()
        if json_data:
            msgs.append(Message.to_all(value=json_data.encode()))
        else:
            msgs.append(Message.to_drop())
        return msgs

    def dumps(self, udf_content_type: str) -> str:
        if udf_content_type == APPLICATION_JSON:
            return json.dumps(self._messages, cls=NumaflowJSONEncoder, separators=(",", ":"))
        elif udf_content_type == APPLICATION_MSG_PACK:
            return msgpack.dumps(self._messages, default=msgpack_encoding)
        raise MarshalError(udf_content_type)

    def loads(self) -> Ms:
        pass
