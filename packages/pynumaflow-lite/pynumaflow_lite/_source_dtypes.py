from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator
from pynumaflow_lite.sourcer import (
    Message,
    Offset,
    ReadRequest,
    AckRequest,
    NackRequest,
    PendingResponse,
    PartitionsResponse,
)


class Sourcer(metaclass=ABCMeta):
    """
    Provides an interface to write a User Defined Source.
    
    A Sourcer must implement the following handlers:
    - read_handler: Read messages from the source
    - ack_handler: Acknowledge processed messages
    - pending_handler: Return the number of pending messages
    - partitions_handler: Return the partitions this source handles
    
    Optionally, you can implement:
    - nack_handler: Negatively acknowledge messages (default: no-op)
    """

    def __call__(self, *args, **kwargs):
        """
        This allows to execute the handler function directly if
        class instance is sent as a callable.
        """
        return self.read_handler(*args, **kwargs)

    @abstractmethod
    async def read_handler(self, request: ReadRequest) -> AsyncIterator[Message]:
        """
        Read messages from the source.
        
        Args:
            request: ReadRequest containing num_records and timeout
            
        Yields:
            Message: Messages to be sent to the next vertex
            
        Example:
            async def read_handler(self, request: ReadRequest) -> AsyncIterator[Message]:
                for i in range(request.num_records):
                    yield Message(
                        payload=f"message-{i}".encode(),
                        offset=Offset(str(i).encode(), partition_id=0),
                        event_time=datetime.now(),
                        keys=["key1"],
                        headers={"x-txn-id": str(uuid.uuid4())}
                    )
        """
        pass

    @abstractmethod
    async def ack_handler(self, request: AckRequest) -> None:
        """
        Acknowledge that messages have been processed.
        
        Args:
            request: AckRequest containing the list of offsets to acknowledge
            
        Example:
            async def ack_handler(self, request: AckRequest) -> None:
                for offset in request.offsets:
                    # Remove from pending set, mark as processed, etc.
                    self.pending_offsets.remove(offset.offset)
        """
        pass

    @abstractmethod
    async def pending_handler(self) -> PendingResponse:
        """
        Return the number of pending messages yet to be processed.
        
        Returns:
            PendingResponse: Response containing the count of pending messages.
                            Return count=-1 if the source doesn't support detecting backlog.
            
        Example:
            async def pending_handler(self) -> PendingResponse:
                return PendingResponse(count=len(self.pending_offsets))
        """
        pass

    @abstractmethod
    async def partitions_handler(self) -> PartitionsResponse:
        """
        Return the partitions associated with this source.
        
        This is used by the platform to determine the partitions to which
        the watermark should be published. If your source doesn't have the
        concept of partitions, return the replica ID.
        
        Returns:
            PartitionsResponse: Response containing the list of partition IDs
            
        Example:
            async def partitions_handler(self) -> PartitionsResponse:
                return PartitionsResponse(partitions=[self.partition_id])
        """
        pass

    async def nack_handler(self, request: NackRequest) -> None:
        """
        Negatively acknowledge messages (optional).
        
        This is called when messages could not be processed and should be
        retried or handled differently. Default implementation is a no-op.
        
        Args:
            request: NackRequest containing the list of offsets to nack
            
        Example:
            async def nack_handler(self, request: NackRequest) -> None:
                for offset in request.offsets:
                    # Add back to pending, mark for retry, etc.
                    self.nacked_offsets.add(offset.offset)
        """
        pass

