import uuid
from datetime import datetime
import logging

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
    NackRequest,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class AsyncSource(Sourcer):
    """
    AsyncSource is a class for User Defined Source implementation.
    """

    def __init__(self):
        # The offset idx till where the messages have been read
        self.read_idx: int = 0
        # Set to maintain a track of the offsets yet to be acknowledged
        self.to_ack_set: set[int] = set()
        # Set to maintain a track of the offsets that have been negatively acknowledged
        self.nacked: set[int] = set()

    async def read_handler(self, datum: ReadRequest, output: NonBlockingIterator):
        """
        read_handler is used to read the data from the source and send the data forward
        for each read request we process num_records and increment the read_idx to indicate that
        the message has been read and the same is added to the ack set
        """
        if self.to_ack_set:
            return

        for x in range(datum.num_records):
            # If there are any nacked offsets, re-deliver them
            if self.nacked:
                idx = self.nacked.pop()
            else:
                idx = self.read_idx
                self.read_idx += 1
            headers = {"x-txn-id": str(uuid.uuid4())}
            await output.put(
                Message(
                    payload=str(self.read_idx).encode(),
                    offset=Offset.offset_with_default_partition_id(str(idx).encode()),
                    event_time=datetime.now(),
                    headers=headers,
                )
            )
            self.to_ack_set.add(idx)

    async def ack_handler(self, ack_request: AckRequest):
        """
        The ack handler is used acknowledge the offsets that have been read, and remove them
        from the to_ack_set
        """
        for req in ack_request.offsets:
            offset = int(req.offset)
            self.to_ack_set.remove(offset)

    async def nack_handler(self, ack_request: NackRequest):
        """
        Add the offsets that have been negatively acknowledged to the nacked set
        """

        for req in ack_request.offsets:
            offset = int(req.offset)
            self.to_ack_set.remove(offset)
            self.nacked.add(offset)
        logger.info("Negatively acknowledged offsets: %s", self.nacked)

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
    logger.info("Starting grpc server")
    grpc_server.start()
