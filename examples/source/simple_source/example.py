import uuid

from collections.abc import Iterable
from datetime import datetime

from pynumaflow.sourcer import (
    ReadRequest,
    Message,
    AckRequest,
    PendingResponse,
    Offset,
    PartitionsResponse,
    get_default_partitions,
    Sourcer,
    SourceServer,
)


class SimpleSource(Sourcer):
    """
    SimpleSource is a class for User Defined Source implementation.
    """

    def __init__(self):
        """
        to_ack_set: Set to maintain a track of the offsets yet to be acknowledged
        read_idx : the offset idx till where the messages have been read
        """
        self.to_ack_set = set()
        self.read_idx = 0

    def read_handler(self, datum: ReadRequest) -> Iterable[Message]:
        """
        read_handler is used to read the data from the source and send the data forward
        for each read request we process num_records and increment the read_idx to indicate that
        the message has been read and the same is added to the ack set
        """
        if self.to_ack_set:
            return

        headers = {"x-txn-id": str(uuid.uuid4())}

        for x in range(datum.num_records):
            yield Message(
                payload=str(self.read_idx).encode(),
                offset=Offset.offset_with_default_partition_id(str(self.read_idx).encode()),
                event_time=datetime.now(),
                headers=headers,
            )
            self.to_ack_set.add(str(self.read_idx))
            self.read_idx += 1

    def ack_handler(self, ack_request: AckRequest):
        """
        The ack handler is used acknowledge the offsets that have been read, and remove them
        from the to_ack_set
        """
        for offset in ack_request.offset:
            self.to_ack_set.remove(str(offset.offset, "utf-8"))

    def pending_handler(self) -> PendingResponse:
        """
        The simple source always returns zero to indicate there is no pending record.
        """
        return PendingResponse(count=0)

    def partitions_handler(self) -> PartitionsResponse:
        """
        The simple source always returns zero to indicate there is no pending record.
        """
        return PartitionsResponse(partitions=get_default_partitions())


if __name__ == "__main__":
    ud_source = SimpleSource()
    grpc_server = SourceServer(ud_source)
    grpc_server.start()
