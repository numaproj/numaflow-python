from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator

from pynumaflow_lite.reducer import Datum, Message, Metadata


class Reducer(metaclass=ABCMeta):
    """
    Interface for reduce handlers. A new instance will be created per window.
    """

    def __call__(self, *args, **kwargs):
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(self, keys: list[str], datums: AsyncIterator[Datum], md: Metadata) -> list[Message]:
        """
        Implement this handler; consume `datums` async iterator and return the messages.
        """
        pass
