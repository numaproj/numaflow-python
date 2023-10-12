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

to_ack_set = set()
read_idx = 0


def read_handler(datum: Datum) -> Iterable[Message]:
    global read_idx
    if len(to_ack_set) > 0:
        return

    for x in range(datum.num_records):
        yield Message(
            payload=bytes(str(read_idx), "utf-8"),
            offset=Offset(offset=bytes(str(read_idx), "utf-8"), partition_id="0"),
            event_time=datetime.now(),
        )
        to_ack_set.add(str(read_idx))
        read_idx = read_idx + 1


def ack_handler(ack_request: AckRequest):
    for offset in ack_request.offset:
        to_ack_set.remove(str(offset.offset, "utf-8"))


def pending_handler() -> PendingResponse:
    return PendingResponse(count=0)


if __name__ == "__main__":
    grpc_server = Sourcer(
        read_handler=read_handler, ack_handler=ack_handler, pending_handler=pending_handler
    )
    grpc_server.start()
