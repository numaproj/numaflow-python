from collections.abc import Iterable
from datetime import datetime

from pynumaflow.sourcer import (
    Datum,
    Message,
    Sourcer,
    AckRequest,
    PendingResponse,
    Offset,
)

TO_ACK_SET: set[str] = set()
READ_IDX = 0


def read_handler(datum: Datum) -> Iterable[Message]:
    global READ_IDX
    if TO_ACK_SET:
        return

    for x in range(datum.num_records):
        yield Message(
            payload=str(READ_IDX).encode(),
            offset=Offset(offset=str(READ_IDX).encode(), partition_id="0"),
            event_time=datetime.now(),
        )
        TO_ACK_SET.add(str(READ_IDX))
        READ_IDX = READ_IDX + 1


def ack_handler(ack_request: AckRequest):
    for offset in ack_request.offset:
        TO_ACK_SET.remove(str(offset.offset, "utf-8"))


def pending_handler() -> PendingResponse:
    return PendingResponse(count=0)


if __name__ == "__main__":
    grpc_server = Sourcer(
        read_handler=read_handler, ack_handler=ack_handler, pending_handler=pending_handler
    )
    grpc_server.start()
