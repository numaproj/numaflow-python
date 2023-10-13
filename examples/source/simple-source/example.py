from collections.abc import Iterable
from datetime import datetime

from pynumaflow.sourcer import (
    ReadRequest,
    Message,
    Sourcer,
    AckRequest,
    PendingResponse,
    Offset,
)


class SimpleSource:
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

        for x in range(datum.num_records):
            yield Message(
                payload=str(self.read_idx).encode(),
                offset=Offset(offset=str(self.read_idx).encode(), partition_id="0"),
                event_time=datetime.now(),
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


if __name__ == "__main__":
    ud_source = SimpleSource()
    grpc_server = Sourcer(
        read_handler=ud_source.read_handler,
        ack_handler=ud_source.ack_handler,
        pending_handler=ud_source.pending_handler,
    )
    grpc_server.start()
