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


class UDSource:
    TO_ACK_SET: set[str] = set()
    READ_IDX = 0

    @classmethod
    def read_handler(cls, datum: ReadRequest) -> Iterable[Message]:
        if cls.TO_ACK_SET:
            return

        for x in range(datum.num_records):
            yield Message(
                payload=str(cls.READ_IDX).encode(),
                offset=Offset(offset=str(cls.READ_IDX).encode(), partition_id="0"),
                event_time=datetime.now(),
            )
            cls.TO_ACK_SET.add(str(cls.READ_IDX))
            cls.READ_IDX = cls.READ_IDX + 1

    @classmethod
    def ack_handler(cls, ack_request: AckRequest):
        for offset in ack_request.offset:
            cls.TO_ACK_SET.remove(str(offset.offset, "utf-8"))

    @classmethod
    def pending_handler(cls) -> PendingResponse:
        return PendingResponse(count=0)


if __name__ == "__main__":
    uds = UDSource()
    grpc_server = Sourcer(
        read_handler=uds.read_handler,
        ack_handler=uds.ack_handler,
        pending_handler=uds.pending_handler,
    )
    grpc_server.start()
