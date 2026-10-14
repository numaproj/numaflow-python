from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator

from pynumaflow_lite.reducestreamer import Datum, Message, Metadata


class ReduceStreamer(metaclass=ABCMeta):
    """
    Interface for reduce streaming handlers. A new instance will be created per window.

    Unlike regular Reducer which returns all messages at once, ReduceStreamer
    allows you to yield messages incrementally as an async iterator.
    """

    def __call__(self, *args, **kwargs):
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(self, keys: list[str], datums: AsyncIterator[Datum], md: Metadata) -> AsyncIterator[Message]:
        """
        Implement this handler; consume `datums` async iterator and yield Messages incrementally.

        Args:
            keys: List of keys for this window
            datums: An async iterator of Datum objects
            md: Metadata containing window information

        Yields:
            Message objects to be sent to the next vertex
        """
        raise NotImplementedError
        yield  # makes this an async generator, so overrides that yield type-check
