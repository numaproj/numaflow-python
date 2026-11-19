from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator

from pynumaflow_lite.sourcer import Message, NackOffset, Offset, ReadRequest


class Sourcer(metaclass=ABCMeta):
    """
    Provides an interface to write a User Defined Source.

    Subclasses must implement `read`, `ack`, `nack`, and `pending`. The
    partition methods have defaults.
    """

    @abstractmethod
    async def read(self, request: ReadRequest) -> AsyncIterator[Message]:
        """
        Yield at most `request.num_records` messages, and return within
        `request.timeout`.

        Example:
            async def read(self, request: ReadRequest) -> AsyncIterator[Message]:
                for _ in range(request.num_records):
                    offset = Offset(str(self.next_id).encode())
                    self.next_id += 1
                    yield Message(b"payload", offset, datetime.now(UTC))
        """
        raise NotImplementedError
        yield  # makes this an async generator, so overrides that yield type-check

    @abstractmethod
    async def ack(self, offsets: list[Offset]) -> None:
        """
        Acknowledge that the messages with these offsets have been processed.
        """

    @abstractmethod
    async def nack(self, offsets: list[NackOffset]) -> None:
        """
        Negatively acknowledge messages, so that they are read again.

        Each entry pairs an offset with the nack options for it.
        """

    @abstractmethod
    async def pending(self) -> int | None:
        """
        Return the number of messages that are yet to be read, or None if the
        source cannot tell.
        """

    async def active_partitions(self) -> list[int] | None:
        """
        Return the partitions that this source currently reads from. The platform
        publishes watermarks to these partitions.

        The default, None, makes the platform use the replica index as the only
        partition.
        """
        return None

    async def total_partitions(self) -> int | None:
        """
        Return the total number of partitions of the source, across all
        replicas, or None if the source cannot tell. The platform uses it to
        know when every partition has reported a watermark.
        """
        return None
