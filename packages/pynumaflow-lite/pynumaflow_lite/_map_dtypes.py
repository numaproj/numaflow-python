from abc import ABCMeta, abstractmethod

from pynumaflow_lite.mapper import Datum, Message


class Mapper(metaclass=ABCMeta):
    """
    Provides an interface to write a Map servicer.
    """

    def __call__(self, *args, **kwargs):
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(self, datum: Datum) -> list[Message]:
        """
        Implement this handler function for map.
        Process the datum and return the messages to forward.
        """
        pass
