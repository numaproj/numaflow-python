from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator

from pynumaflow_lite.session_reducer import Datum, Message


class SessionReducer(metaclass=ABCMeta):
    """
    Interface for session reduce handlers. A new instance will be created per keyed window.
    """

    @abstractmethod
    async def session_reduce(
            self, keys: list[str], datums: AsyncIterator[Datum]
    ) -> AsyncIterator[Message]:
        """
        Implement this handler; consume `datums` async iterable and yield Messages.
        This is called for each session window.
        """
        pass

    @abstractmethod
    async def accumulator(self) -> bytes:
        """
        Return the current state as bytes. Called when this session is merged with another.
        """
        pass

    @abstractmethod
    async def merge_accumulator(self, accumulator: bytes) -> None:
        """
        Merge the given accumulator (from another session) into this session's state.
        """
        pass
