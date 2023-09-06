from dataclasses import dataclass
from typing import TypeVar

R = TypeVar("R", bound="Response")


@dataclass
class Response:
    """
    Class to define the important information for the event.
    Args:
        value: the payload of the event.
        no_broadcast: the flag to indicate whether the event should be broadcasted.
    >>> # Example usage
    >>> Response.broadcast_message(b"hello")
    >>> Response.no_broadcast_message()
    """

    __slots__ = ("value", "no_broadcast")

    value: bytes
    no_broadcast: bool

    @classmethod
    def broadcast_message(cls: type[R], value: bytes) -> R:
        """
        Returns a SideInputResponse object with the given value,
        and the No broadcast flag set to False. This event will be broadcasted.
        """
        return Response(value=value, no_broadcast=False)

    @classmethod
    def no_broadcast_message(cls: type[R]) -> R:
        """
        Returns a SideInputResponse object with the No broadcast flag set to True.
        This event will not be broadcasted.
        """
        return Response(value=b"", no_broadcast=True)
