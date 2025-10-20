from abc import ABCMeta, abstractmethod
from typing import AsyncIterator

from pynumaflow_lite.accumulator import Datum, Message


class Accumulator(metaclass=ABCMeta):
    """
    Provides an interface to write an Accumulator
    which will be exposed over a gRPC server.
    """

    def __call__(self, *args, **kwargs):
        """
        This allows to execute the handler function directly if
        class instance is sent as a callable.
        """
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(self, datums: AsyncIterator[Datum]) -> AsyncIterator[Message]:
        """
        Accumulate can read unordered from the input stream and emit the ordered data to the output stream.
        Once the watermark (WM) of the output stream progresses, the data in WAL until that WM will be garbage collected.
        NOTE: A message can be silently dropped if need be, and it will be cleared from the WAL when the WM progresses.

        Args:
            datums: An async iterator of Datum objects

        Yields:
            Message objects to be sent to the next vertex
        """
        pass

