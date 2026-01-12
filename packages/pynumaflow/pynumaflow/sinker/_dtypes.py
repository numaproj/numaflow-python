from abc import abstractmethod, ABCMeta
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar, Optional, Callable, Union
from collections.abc import AsyncIterable, Awaitable
from collections.abc import Sequence, Iterator
from warnings import warn

from pynumaflow._metadata import SystemMetadata, UserMetadata

R = TypeVar("R", bound="Response")
Rs = TypeVar("Rs", bound="Responses")


@dataclass
class Message:
    """
    Basic datatype for OnSuccess UDSink message.

    Args:
        keys: list of keys for the on_success message.
        value: payload of the on_success message.
        user_metadata: user metadata of the on_success message.
    """

    _keys: Optional[list[str]]
    _value: bytes
    _user_metadata: Optional[UserMetadata]

    __slots__ = ("_keys", "_value", "_user_metadata")

    def __init__(
        self,
        value: bytes,
        keys: Optional[list[str]] = None,
        user_metadata: Optional[UserMetadata] = None,
    ):
        self._value = value
        self._keys = keys
        self._user_metadata = user_metadata

    def with_keys(self, keys: Optional[list[str]]):
        self._keys = keys
        return self

    def with_user_metadata(self, user_metadata: Optional[UserMetadata]):
        self._user_metadata = user_metadata
        return self

    @property
    def keys(self) -> Optional[list[str]]:
        """Returns the id of the event."""
        return self._keys

    @property
    def value(self) -> bytes:
        """Returns the id of the event."""
        return self._value

    @property
    def user_metadata(self) -> Optional[UserMetadata]:
        """Returns the id of the event."""
        return self._user_metadata


@dataclass
class Response:
    """
    Basic datatype for UDSink response.

    Args:
        id: the id of the event.
        success: boolean indicating whether the event was successfully processed.
        err: error message if the event was not successfully processed.
        fallback: fallback is true if the message to be sent to the fallback sink.
    """

    id: str
    success: bool
    err: Optional[str]
    fallback: bool
    on_success: bool
    on_success_msg: Optional[Message]

    __slots__ = ("id", "success", "err", "fallback", "on_success", "on_success_msg")

    # as_success creates a successful Response with the given id.
    # The Success field is set to true.
    @classmethod
    def as_success(cls, id_: str) -> "Response":
        return Response(
            id=id_, success=True, err=None, fallback=False, on_success=False, on_success_msg=None
        )

    # as_failure creates a failed Response with the given id and error message.
    # The success field is set to false and the err field is set to the provided error message.
    @classmethod
    def as_failure(cls, id_: str, err_msg: str) -> "Response":
        return Response(
            id=id_,
            success=False,
            err=err_msg,
            fallback=False,
            on_success=False,
            on_success_msg=None,
        )

    # as_fallback creates a Response with the fallback field set to true.
    # This indicates that the message should be sent to the fallback sink.
    @classmethod
    def as_fallback(cls, id_: str) -> "Response":
        return Response(
            id=id_, fallback=True, err=None, success=False, on_success=False, on_success_msg=None
        )

    # as_on_success creates a Response with the on_success field set to true.
    # This indicates that the message should be sent to the on_success sink.
    @classmethod
    def as_on_success(cls, id_: str, on_success: Optional[Message] = None) -> "Response":
        return Response(
            id=id_,
            fallback=False,
            err=None,
            success=False,
            on_success=True,
            on_success_msg=on_success,
        )


class Responses(Sequence[R]):
    """
    Container to hold a list of Response instances.

    Args:
        responses: list of Response instances.
    """

    __slots__ = ("_responses",)

    def __init__(self, *responses: R):
        self._responses = list(responses) or []

    def __str__(self) -> str:
        return str(self._responses)

    def __repr__(self) -> str:
        return str(self)

    def __len__(self) -> int:
        return len(self._responses)

    def __iter__(self) -> Iterator[R]:
        return iter(self._responses)

    def __getitem__(self, index: int) -> R:
        if isinstance(index, slice):
            raise TypeError("Slicing is not supported for Responses")
        return self._responses[index]

    def append(self, response: R) -> None:
        self._responses.append(response)

    def items(self) -> list[R]:
        warn(
            "Using items is deprecated and will be removed in v0.5. "
            "Iterate or index the Responses object instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._responses


@dataclass(init=False, repr=False)
class Datum:
    """
    Class to define the important information for the event.

    Args:
        keys: the keys of the event.
        value: the payload of the event.
        event_time: the event time of the event.
        watermark: the watermark of the event.
        headers: the headers of the event.

    Example usage
    ```py
    from pynumaflow.sinker import Datum
    from datetime import datetime, timezone

    d = Datum(
          keys=["test_key"],
          sink_msg_id="test_id",
          value=b"test_mock_message",
          event_time=datetime.fromtimestamp(1662998400, timezone.utc),
          watermark=datetime.fromtimestamp(1662998460, timezone.utc),
          headers={"key1": "value1", "key2": "value2"},
       )
    ```
    """

    __slots__ = (
        "_keys",
        "_id",
        "_value",
        "_event_time",
        "_watermark",
        "_headers",
        "_user_metadata",
        "_system_metadata",
    )

    _keys: list[str]
    _id: str
    _value: bytes
    _event_time: datetime
    _watermark: datetime
    _headers: dict[str, str]
    _user_metadata: UserMetadata
    _system_metadata: SystemMetadata

    def __init__(
        self,
        keys: list[str],
        sink_msg_id: str,
        value: bytes,
        event_time: datetime,
        watermark: datetime,
        headers: Optional[dict[str, str]] = None,
        user_metadata: Optional[UserMetadata] = None,
        system_metadata: Optional[SystemMetadata] = None,
    ):
        self._keys = keys
        self._id = sink_msg_id or ""
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

    def __str__(self):
        value_string = self._value.decode("utf-8")
        return (
            f"keys: {self._keys}, "
            f"id: {self._id}, value: {value_string}, "
            f"event_time: {str(self._event_time)}, "
            f"watermark: {str(self._watermark)}, "
            f"headers: {self._headers}"
        )

    def __repr__(self):
        return str(self)

    @property
    def id(self) -> str:
        """Returns the id of the event."""
        return self._id

    @property
    def keys(self) -> list[str]:
        """Returns the keys of the event."""
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


class Sinker(metaclass=ABCMeta):
    """
    Provides an interface to write a Sinker
    which will be exposed over a gRPC server.

    """

    def __call__(self, *args, **kwargs):
        """
        Allow to call handler function directly if class instance is sent
        as the sinker_instance.
        """
        return self.handler(*args, **kwargs)

    @abstractmethod
    def handler(self, datums: Iterator[Datum]) -> Responses:
        """
        Implement this handler function which implements the SinkCallable interface.
        """
        pass


# SinkSyncCallable is a callable which can be used as a handler for the Synchronous UDSink.
SinkHandlerCallable = Callable[[Iterator[Datum]], Responses]
SinkSyncCallable = Union[Sinker, SinkHandlerCallable]

# SinkAsyncCallable is a callable which can be used as a handler for the Asynchronous UDSink.
AsyncSinkHandlerCallable = Callable[[AsyncIterable[Datum]], Awaitable[Responses]]
SinkAsyncCallable = Union[Sinker, AsyncSinkHandlerCallable]
