from abc import ABCMeta, abstractmethod
from pynumaflow_lite.reducer import Datum, Messages, Metadata
from collections.abc import AsyncIterable


class Reducer(metaclass=ABCMeta):
    """
    Interface for reduce handlers. A new instance will be created per window.
    """

    def __call__(self, *args, **kwargs):
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(self, keys: list[str], datums: AsyncIterable[Datum], md: Metadata) -> Messages:
        """
        Implement this handler; consume `datums` async iterable and return Messages.
        """
        pass

