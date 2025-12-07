from abc import ABCMeta, abstractmethod
from pynumaflow_lite.sideinputer import Response


class SideInput(metaclass=ABCMeta):
    """
    Provides an interface to write a SideInput retriever
    which will be exposed over a gRPC server.

    A SideInput is used for periodically retrieving data that can be
    broadcast to other vertices in the pipeline.
    """

    async def __call__(self, *args, **kwargs):
        """
        This allows to execute the handler function directly if
        class instance is sent as a callable.
        """
        return await self.retrieve_handler(*args, **kwargs)

    @abstractmethod
    async def retrieve_handler(self) -> Response:
        """
        Implement this handler function which implements the SideInput interface.

        This function is called every time the side input is requested.

        Returns:
            Response: Either Response.broadcast_message(value) to broadcast a value,
                     or Response.no_broadcast_message() to skip broadcasting.
        """
        pass

