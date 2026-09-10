from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterable

from pynumaflow_lite.sinker import Datum, Response


class Sinker(metaclass=ABCMeta):
    """
    Provides an interface to write a Sink servicer.
    """

    def __call__(self, *args, **kwargs):
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(self, datums: AsyncIterable[Datum]) -> list[Response]:
        """
        Implement this handler function for sink.
        Process the stream of datums and return responses.
        """
        pass
