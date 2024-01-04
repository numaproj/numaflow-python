from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar, Optional, Callable
from collections.abc import Sequence, Iterator
from warnings import warn

R = TypeVar("R", bound="Response")
Rs = TypeVar("Rs", bound="Responses")


@dataclass
class Response:
    """
    Basic datatype for UDSink response.

    Args:
        id: the id of the event.
        success: boolean indicating whether the event was successfully processed.
        err: error message if the event was not successfully processed.
    """

    id: str
    success: bool
    err: Optional[str]

    __slots__ = ("id", "success", "err")

    @classmethod
    def as_success(cls: type[R], id_: str) -> R:
        return Response(id=id_, success=True, err=None)

    @classmethod
    def as_failure(cls: type[R], id_: str, err_msg: str) -> R:
        return Response(id=id_, success=False, err=err_msg)


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
    >>> # Example usage
    >>> from pynumaflow.sinker import Datum
    >>> from datetime import datetime, timezone
    >>> payload = bytes("test_mock_message", encoding="utf-8")
    >>> t1 = datetime.fromtimestamp(1662998400, timezone.utc)
    >>> t2 = datetime.fromtimestamp(1662998460, timezone.utc)
    >>> msg_id = "test_id"
    >>> output_keys = ["test_key"]
    >>> d = Datum(keys=output_keys, sink_msg_id=msg_id, value=payload, event_time=t1, watermark=t2)
    """

    __slots__ = ("_keys", "_id", "_value", "_event_time", "_watermark")

    _keys: list[str]
    _id: str
    _value: bytes
    _event_time: datetime
    _watermark: datetime

    def __init__(
        self,
        keys: list[str],
        sink_msg_id: str,
        value: bytes,
        event_time: datetime,
        watermark: datetime,
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

    def __str__(self):
        value_string = self._value.decode("utf-8")
        return (
            f"keys: {self._keys}, "
            f"id: {self._id}, value: {value_string}, "
            f"event_time: {str(self._event_time)}, "
            f"watermark: {str(self._watermark)}"
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


SinkCallable = Callable[[Iterator[Datum]], Responses]
