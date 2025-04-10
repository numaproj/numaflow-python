import uuid
from datetime import datetime

from pynumaflow.shared.asynciter import NonBlockingIterator
from pynumaflow.sourcer import (
    ReadRequest,
    Message,
    AckRequest,
    PendingResponse,
    Offset,
    PartitionsResponse,
    get_default_partitions,
    Sourcer,
    SourceAsyncServer,
)


class AsyncSource(Sourcer):
    """
    AsyncSource is a class for User Defined Source implementation.
    """

    def __init__(self):
        """
        to_ack_set: Set to maintain a track of the offsets yet to be acknowledged
        read_idx : the offset idx till where the messages have been read
        """
        self.to_ack_set = set()
        self.read_idx = 0

    async def read_handler(self, datum: ReadRequest, output: NonBlockingIterator):
        """
        read_handler is used to read the data from the source and send the data forward
        for each read request we process num_records and increment the read_idx to indicate that
        the message has been read and the same is added to the ack set
        """
        if self.to_ack_set:
            return

        for x in range(datum.num_records):
            headers = {"x-txn-id": str(uuid.uuid4())}
            await output.put(
                Message(
                    payload=str(self.read_idx).encode(),
                    offset=Offset.offset_with_default_partition_id(str(self.read_idx).encode()),
                    event_time=datetime.now(),
                    headers=headers,
                )
            )
            self.to_ack_set.add(str(self.read_idx))
            self.read_idx += 1

    async def ack_handler(self, ack_request: AckRequest):
        """
        The ack handler is used acknowledge the offsets that have been read, and remove them
        from the to_ack_set
        """
        for req in ack_request.offsets:
            self.to_ack_set.remove(str(req.offset, "utf-8"))

    async def pending_handler(self) -> PendingResponse:
        """
        The simple source always returns zero to indicate there is no pending record.
        """
        return PendingResponse(count=0)

    async def partitions_handler(self) -> PartitionsResponse:
        """
        The simple source always returns default partitions.
        """
        return PartitionsResponse(partitions=get_default_partitions())


if __name__ == "__main__":
    ud_source = AsyncSource()
    grpc_server = SourceAsyncServer(ud_source)
    grpc_server.start()
