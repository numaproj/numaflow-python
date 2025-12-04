from abc import ABCMeta, abstractmethod
from pynumaflow_lite.sourcetransformer import Datum, Messages


class SourceTransformer(metaclass=ABCMeta):
    """
    Provides an interface to write a SourceTransformer
    which will be exposed over a gRPC server.
    
    A SourceTransformer is used for transforming and assigning event time
    to input messages from a source.
    """

    def __call__(self, *args, **kwargs):
        """
        This allows to execute the handler function directly if
        class instance is sent as a callable.
        """
        return self.handler(*args, **kwargs)

    @abstractmethod
    async def handler(self, keys: list[str], datum: Datum) -> Messages:
        """
        Implement this handler function which implements the SourceTransformer interface.
        
        Args:
            keys: The keys associated with the message.
            datum: The input datum containing value, event_time, watermark, and headers.
            
        Returns:
            Messages: A collection of transformed messages with potentially modified
                     event times and tags for conditional forwarding.
        """
        pass

