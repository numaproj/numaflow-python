from abc import ABCMeta, abstractmethod
from pynumaflow_lite.batchmapper import Datum, BatchResponse
from collections.abc import AsyncIterable


class BatchMapper(metaclass=ABCMeta):
    """
    Provides an interface to write a BatchMap servicer.
    """

    def __call__(self, *args, **kwargs):
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(self, batch: AsyncIterable[Datum]) -> list[BatchResponse]:
        """
        Implement this handler function for batch mapping.
        The returned list length should equal the input batch size.
        """
        pass
