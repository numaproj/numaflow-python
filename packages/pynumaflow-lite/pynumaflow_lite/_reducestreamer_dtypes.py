from abc import ABCMeta, abstractmethod
from pynumaflow_lite.reducestreamer import Datum, Message, Metadata
from collections.abc import AsyncIterable, AsyncIterator


class ReduceStreamer(metaclass=ABCMeta):
    """
    Interface for reduce streaming handlers. A new instance will be created per window.
    
    Unlike regular Reducer which returns all messages at once, ReduceStreamer
    allows you to yield messages incrementally as an async iterator.
    """

    def __call__(self, *args, **kwargs):
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(
        self, 
        keys: list[str], 
        datums: AsyncIterable[Datum], 
        md: Metadata
    ) -> AsyncIterator[Message]:
        """
        Implement this handler; consume `datums` async iterable and yield Messages incrementally.
        
        Args:
            keys: List of keys for this window
            datums: An async iterator of Datum objects
            md: Metadata containing window information
            
        Yields:
            Message objects to be sent to the next vertex
        """
        pass

