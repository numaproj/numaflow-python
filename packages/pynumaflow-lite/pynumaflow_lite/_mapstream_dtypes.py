from abc import ABCMeta, abstractmethod
from pynumaflow_lite.mapstreamer import Datum, Message
from collections.abc import AsyncIterator


class MapStreamer(metaclass=ABCMeta):
    """
    Provides an interface to write a streaming map servicer.
    The handler yields outputs incrementally as an async iterator.
    """

    def __call__(self, *args, **kwargs):
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(self, keys: list[str], datum: Datum) -> AsyncIterator[Message]:
        """
        Implement this handler function for streaming mapping.
        It should be an async generator yielding Message objects.
        """
        pass

