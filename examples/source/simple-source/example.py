from datetime import datetime

from pynumaflow.sourcer import (
    Datum,
    Message,
    Sourcer,
    AckRequest,
    PendingResponse,
    Offset,
    Messages,
)

to_ack_set = set()
read_idx = 0


def read_handler(datum: Datum) -> Messages:
    global read_idx
    if len(to_ack_set) > 0:
        return

    for x in range(datum.num_records):
        yield Message(
            payload=bytes(str(read_idx), 'utf-8'),
            offset=Offset(offset=bytes(str(read_idx), 'utf-8'), partition_id="0"),
            event_time=datetime.now(),
        )
        to_ack_set.add(read_idx)
        read_idx = read_idx + 1


def ack_handler(ack_request: AckRequest):
    print("ack_handler", type(ack_request))
    print("ack_handler_2", ack_request)
    for offset in ack_request.offset:
        print("ack_handler", offset.offset)
        # to_ack_set.remove(offset.offset)


def pending_handler() -> PendingResponse:
    return PendingResponse(count=0)


if __name__ == "__main__":
    grpc_server = Sourcer(
        read_handler=read_handler, ack_handler=ack_handler, pending_handler=pending_handler
    )
    grpc_server.start()
