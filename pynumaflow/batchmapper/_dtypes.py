from abc import ABCMeta, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar, Callable, Union, Optional
from collections.abc import Awaitable
from warnings import warn

from pynumaflow._constants import DROP

M = TypeVar("M", bound="Message")
Ms = TypeVar("Ms", bound="Messages")
B = TypeVar("B", bound="BatchResponse")
Bs = TypeVar("Bs", bound="BatchResponses")


@dataclass(init=False)
class Message:
    """
    Basic datatype for data passing to the next vertex/vertices.

    Args:
        value: data in bytes
        keys: []string keys for vertex (optional)
        tags: []string tags for conditional forwarding (optional)
    """

    __slots__ = ("_value", "_keys", "_tags")

    _value: bytes
    _keys: list[str]
    _tags: list[str]

    def __init__(self, value: bytes, keys: list[str] = None, tags: list[str] = None):
        """
        Creates a Message object to send value to a vertex.
        """
        self._keys = keys or []
        self._tags = tags or []
        self._value = value or b""

    # returns the Message Object which will be dropped
    @classmethod
    def to_drop(cls: type[M]) -> M:
        return cls(b"", None, [DROP])

    @property
    def value(self) -> bytes:
        return self._value

    @property
    def keys(self) -> list[str]:
        return self._keys

    @property
    def tags(self) -> list[str]:
        return self._tags


class Messages(Sequence[M]):
    """
    Class to define a list of Message objects.

    Args:
        messages: list of Message objects.
    """

    __slots__ = ("_messages",)

    def __init__(self, *messages: M):
        self._messages = list(messages) or []

    def __str__(self) -> str:
        return str(self._messages)

    def __repr__(self) -> str:
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
        id: the unique ID for this request

    >>> # Example usage
    >>> from pynumaflow.mapstreamer import Datum
    >>> from datetime import datetime, timezone
    >>> payload = bytes("test_mock_message", encoding="utf-8")
    >>> t1 = datetime.fromtimestamp(1662998400, timezone.utc)
    >>> t2 = datetime.fromtimestamp(1662998460, timezone.utc)
    >>> msg_headers = {"key1": "value1", "key2": "value2"}
    >>> d = Datum(
    ...       keys=["test_key"],
    ...       value=payload,
    ...       event_time=t1,
    ...       watermark=t2,
    ...       headers=msg_headers,
    ...    )
    """

    __slots__ = ("_keys", "_value", "_event_time", "_watermark", "_headers", "_id")

    _keys: list[str]
    _value: bytes
    _event_time: datetime
    _watermark: datetime
    _headers: dict[str, str]
    _id: str

    def __init__(
        self,
        id: str,
        keys: list[str],
        value: bytes,
        event_time: datetime,
        watermark: datetime,
        headers: Optional[dict[str, str]] = None,
    ):
        self._id = id
        self._keys = keys or list()
        self._value = value or b""
        if not isinstance(event_time, datetime):
            raise TypeError(f"Wrong data type: {type(event_time)} for Datum.event_time")
        self._event_time = event_time
        if not isinstance(watermark, datetime):
            raise TypeError(f"Wrong data type: {type(watermark)} for Datum.watermark")
        self._watermark = watermark
        self._headers = headers or {}

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
    def id(self) -> str:
        """Returns the id of the event."""
        return self._id


@dataclass
class BatchResponse:
    """
    Basic datatype for Batch map response.

    Args:
        id: the id of the request.
        messages: list of responses for corresponding to the request id
    """

    _id: str
    messages: list[M]

    __slots__ = ("_id", "messages")

    # as_success creates a successful Response with the given id.
    # The Success field is set to true.
    @classmethod
    def new_batch_response(cls: type[B], id_: str) -> B:
        return BatchResponse(_id=id_, messages=[])

    def append(self, message: Message) -> None:
        self.messages.append(message)

    def items(self) -> list[Message]:
        return self.messages

    def id(self) -> str:
        return self._id


class BatchResponses(Sequence[B]):
    """
    Class to define a list of Batch Response objects.

    Args:
        responses: list of Batch Response objects.
    """

    __slots__ = ("_responses",)

    def __init__(self, *responses: B):
        self._responses = list(responses) or []

    def __str__(self) -> str:
        return str(self._responses)

    def __repr__(self) -> str:
        return str(self)

    def __len__(self) -> int:
        return len(self._responses)

    def __iter__(self) -> Iterator[B]:
        return iter(self._responses)

    def __getitem__(self, index: int) -> M:
        if isinstance(index, slice):
            raise TypeError("Slicing is not supported for BatchResponses")
        return self._responses[index]

    def append(self, response: BatchResponse) -> None:
        self._responses.append(response)

    def items(self) -> list[BatchResponse]:
        return self._responses


class BatchMapper(metaclass=ABCMeta):
    """
    Provides an interface to write a Batch Mapper
    which will be exposed over a gRPC server.

    Args:

    """

    def __call__(self, *args, **kwargs):
        """
        Allow to call handler function directly if class instance is sent
        """
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(self, datums: list[Datum]) -> BatchResponses:
        """
        Implement this handler function which implements the BatchMapAsyncCallable interface.
        """
        pass


BatchMapAsyncCallable = Callable[[list[Datum]], Awaitable[BatchResponses]]
BatchMapCallable = Union[BatchMapper, BatchMapAsyncCallable]
