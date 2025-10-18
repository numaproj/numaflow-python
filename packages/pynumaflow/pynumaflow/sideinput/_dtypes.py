from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import TypeVar, Callable, Union

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


class SideInput(metaclass=ABCMeta):
    """
    Provides an interface to write a SideInput Class
    which will be exposed over gRPC.
    """

    def __call__(self, *args, **kwargs):
        """
        This allows to execute the handler function directly if
        class instance is sent as a callable.
        """
        return self.retrieve_handler(*args, **kwargs)

    @abstractmethod
    def retrieve_handler(self) -> Response:
        """
        This function is called when a Side Input request is received.
        """
        pass


RetrieverHandlerCallable = Callable[[], Response]
RetrieverCallable = Union[SideInput, RetrieverHandlerCallable]
