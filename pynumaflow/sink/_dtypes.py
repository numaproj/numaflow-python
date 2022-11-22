from dataclasses import dataclass
from datetime import datetime
from typing import List, TypeVar, Type, Optional

R = TypeVar("R", bound="Response")
Rs = TypeVar("Rs", bound="Responses")


@dataclass(frozen=True)
class Response:
    id: str
    success: bool
    err: Optional[str]

    @classmethod
    def as_success(cls: Type[R], id_: str) -> R:
        return Response(id=id_, success=True, err=None)

    @classmethod
    def as_failure(cls: Type[R], id_: str, err_msg: str) -> R:
        return Response(id=id_, success=False, err=err_msg)


class Responses:
    def __init__(self, *responses: R):
        self._responses = list(responses) or []

    def __str__(self):
        return str(self._responses)

    def __repr__(self):
        return str(self)

    def append(self, response: R) -> None:
        return self._responses.append(response)

    def items(self) -> List[R]:
        return self._responses

    def dumps(self) -> str:
        return str(self)


class Datum:
    """
    Class to define the important information for the event.
    Args:
        value: the payload of the event.
        event_time: the event time of the event.
        watermark: the watermark of the event.
    >>> # Example usage
    >>> from pynumaflow.function import Datum
    >>> from datetime import datetime, timezone
    >>> payload = bytes("test_mock_message", encoding="utf-8")
    >>> t1 = datetime.fromtimestamp(1662998400, timezone.utc)
    >>> t2 = datetime.fromtimestamp(1662998460, timezone.utc)
    >>> msg_id = "test_id"
    >>> d = Datum(sink_msg_id=msg_id, value=payload, event_time=t1, watermark=t2)
    """

    def __init__(self, sink_msg_id: str, value: bytes, event_time: datetime, watermark: datetime):
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
