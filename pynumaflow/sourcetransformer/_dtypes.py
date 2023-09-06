from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar, Callable
from warnings import warn

from pynumaflow._constants import DROP

M = TypeVar("M", bound="Message")
Ms = TypeVar("Ms", bound="Messages")


@dataclass(init=False)
class Message:
    """
    Basic datatype for data passing to the next vertex/vertices.

    Args:
        value: data in bytes
        event_time: event time of the message, usually extracted from the payload.
        keys: []string keys for vertex (optional)
        tags: []string tags for conditional forwarding (optional)
    """

    __slots__ = ("_value", "_keys", "_tags", "_event_time")

    _keys: list[str]
    _tags: list[str]
    _value: bytes
    _event_time: datetime

    def __init__(
        self, value: bytes, event_time: datetime, keys: list[str] = None, tags: list[str] = None
    ):
        """
        Creates a MessageT object to send value to a vertex.
        """
        self._tags = tags or []
        self._keys = keys or []

        # There is no year 0, so setting following as default event time.
        self._event_time = event_time or datetime(1, 1, 1, 0, 0)
        self._value = value or b""

    @classmethod
    def to_drop(cls: type[M]) -> M:
        return cls(b"", datetime(1, 1, 1, 0, 0), None, [DROP])

    @property
    def event_time(self) -> datetime:
        return self._event_time

    @property
    def keys(self) -> list[str]:
        return self._keys

    @property
    def value(self) -> bytes:
        return self._value

    @property
    def tags(self) -> list[str]:
        return self._tags


class Messages(Sequence[M]):
    """
    Class to define a list of MessageT objects.

    Args:
        message_ts: list of MessageT objects.
    """

    __slots__ = ("_message_ts",)

    def __init__(self, *message_ts: M):
        self._message_ts = list(message_ts) or []

    def __str__(self):
        return str(self._message_ts)

    def __repr__(self):
        return str(self)

    def __len__(self) -> int:
        return len(self._message_ts)

    def __iter__(self) -> Iterator[M]:
        return iter(self._message_ts)

    def __getitem__(self, index: int) -> M:
        if isinstance(index, slice):
            raise TypeError("Slicing is not supported for Messages")
        return self._message_ts[index]

    def append(self, message_t: Message) -> None:
        self._message_ts.append(message_t)

    def items(self) -> list[Message]:
        warn(
            "Using items is deprecated and will be removed in v0.5. "
            "Iterate or index the Messages object instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._message_ts


@dataclass(init=False)
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
    >>> from pynumaflow.sourcetransformer import Datum
    >>> from datetime import datetime, timezone
    >>> payload = bytes("test_mock_message", encoding="utf-8")
    >>> t1 = datetime.froMimestamp(1662998400, timezone.utc)
    >>> t2 = datetime.froMimestamp(1662998460, timezone.utc)
    >>> d = Datum(
    ...       keys=["test_key"],
    ...       value=payload,
    ...       event_time=t1,
    ...       watermark=t2,
    ...    )
    """

    __slots__ = ("_keys", "_value", "_event_time", "_watermark")

    _keys: list[str]
    _value: bytes
    _event_time: datetime
    _watermark: datetime

    def __init__(
        self,
        keys: list[str],
        value: bytes,
        event_time: datetime,
        watermark: datetime,
    ):
        self._keys = keys or list()
        self._value = value or b""
        if not isinstance(event_time, datetime):
            raise TypeError(f"Wrong data type: {type(event_time)} for Datum.event_time")
        self._event_time = event_time
        if not isinstance(watermark, datetime):
            raise TypeError(f"Wrong data type: {type(watermark)} for Datum.watermark")
        self._watermark = watermark

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


SourceTransformCallable = Callable[[list[str], Datum], Messages]
