from abc import ABCMeta, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar, Callable, Union, Optional
from collections.abc import Awaitable
from warnings import warn

from pynumaflow._constants import DROP
from pynumaflow._metadata import UserMetadata, SystemMetadata

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
        user_metadata: metadata for the message (optional)
    """

    __slots__ = ("_value", "_keys", "_tags", "_event_time", "_user_metadata")

    _keys: list[str]
    _tags: list[str]
    _value: bytes
    _event_time: datetime
    _user_metadata: UserMetadata

    def __init__(
        self,
        value: bytes,
        event_time: datetime,
        keys: list[str] = None,
        tags: list[str] = None,
        user_metadata: Optional[UserMetadata] = None,
    ):
        """
        Creates a Message object to send value to a vertex.
        """
        self._tags = tags or []
        self._keys = keys or []

        # There is no year 0, so setting following as default event time.
        self._event_time = event_time or datetime(1, 1, 1, 0, 0)
        self._value = value or b""
        self._user_metadata = user_metadata or UserMetadata()

    @classmethod
    def to_drop(cls: type[M], event_time: datetime) -> M:
        return cls(b"", event_time, None, [DROP])

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

    @property
    def user_metadata(self) -> UserMetadata:
        return self._user_metadata


class Messages(Sequence[M]):
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

    def __len__(self) -> int:
        return len(self._messages)

    def __iter__(self) -> Iterator[M]:
        return iter(self._messages)

    def __getitem__(self, index: int) -> M:
        if isinstance(index, slice):
            raise TypeError("Slicing is not supported for Messages")
        return self._messages[index]

    def append(self, message: Message) -> None:
        self._messages.append(message)

    def items(self) -> list[Message]:
        warn(
            "Using items is deprecated and will be removed in v0.5. "
            "Iterate or index the Messages object instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._messages


@dataclass(init=False)
class Datum:
    """
    Class to define the important information for the event.

    Args:
        keys: the keys of the event.
        value: the payload of the event.
        event_time: the event time of the event.
        watermark: the watermark of the event.
        headers: the headers of the event.
        user_metadata: the user metadata of the event.
        system_metadata: the system metadata of the event.

    Example:
    ```py
    from pynumaflow.sourcetransformer import Datum
    from datetime import datetime, timezone

    d = Datum(
            keys=["test_key"],
            value=b"test_mock_message",
            event_time=datetime.fromtimestamp(1662998400, timezone.utc),
            watermark=datetime.fromtimestamp(1662998460, timezone.utc),
            headers={"key1": "value1", "key2": "value2"},
       )
    ```
    """

    __slots__ = (
        "_keys",
        "_value",
        "_event_time",
        "_watermark",
        "_headers",
        "_user_metadata",
        "_system_metadata",
    )

    _keys: list[str]
    _value: bytes
    _event_time: datetime
    _watermark: datetime
    _headers: dict[str, str]
    _user_metadata: UserMetadata
    _system_metadata: SystemMetadata

    def __init__(
        self,
        keys: list[str],
        value: bytes,
        event_time: datetime,
        watermark: datetime,
        headers: Optional[dict[str, str]] = None,
        user_metadata: Optional[UserMetadata] = None,
        system_metadata: Optional[SystemMetadata] = None,
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
        self._user_metadata = user_metadata or UserMetadata()
        self._system_metadata = system_metadata or SystemMetadata()

    @property
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

    @property
    def headers(self) -> dict[str, str]:
        """Returns the headers of the event."""
        return self._headers.copy()

    @property
    def user_metadata(self) -> UserMetadata:
        """Returns the user metadata of the event."""
        return self._user_metadata

    @property
    def system_metadata(self) -> SystemMetadata:
        """Returns the system metadata of the event."""
        return self._system_metadata


class SourceTransformer(metaclass=ABCMeta):
    """
    Provides an interface to write a Source Transformer
    which will be exposed over a GRPC server.
    """

    def __call__(self, *args, **kwargs):
        """
        Allow to call handler function directly if class instance is sent
        as the source_transformer_instance.
        """
        return self.handler(*args, **kwargs)

    @abstractmethod
    def handler(self, keys: list[str], datum: Datum) -> Messages:
        """
        Implement this handler function which implements the
        SourceTransformCallable interface.
        """
        pass


SourceTransformHandler = Callable[[list[str], Datum], Messages]
# SourceTransformCallable is the type of the handler function for the
# Source Transformer UDFunction.
SourceTransformCallable = Union[SourceTransformHandler, SourceTransformer]


# SourceTransformAsyncCallable is a callable which can be used as a handler
# for the Asynchronous Transformer UDF
SourceTransformHandlerAsyncHandlerCallable = Callable[[list[str], Datum], Awaitable[Messages]]
SourceTransformAsyncCallable = Union[SourceTransformer, SourceTransformHandlerAsyncHandlerCallable]
