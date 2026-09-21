from abc import ABCMeta, abstractmethod

from pynumaflow_lite.mapper import Datum, Message


class Mapper(metaclass=ABCMeta):
    """
    Provides an interface to write a SyncMapServicer
    which will be exposed over a Synchronous gRPC server.
    """

    def __call__(self, *args, **kwargs):
        """
        This allows to execute the handler function directly if
        class instance is sent as a callable.
        """
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(self, payload: Datum) -> list[Message]:
        """
        Implement this handler function which implements the MapAsyncCallable interface.
        """
        pass
