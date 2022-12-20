from typing import Iterator
from pynumaflow.function import Messages, Message, Datum, Metadata, UserDefinedFunctionServicer


def map_handler(key: str, datum: Datum) -> Messages:
    # forward a message
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    messages.append(Message.to_vtx(key, val))
    return messages


def reduce_handler(key: str, datums: Iterator[Datum], md: Metadata) -> Messages:
    # count the number of events
    interval_window = md.interval_window
    counter = 0
    for _ in datums:
        counter += 1
    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    return Messages(Message.to_vtx(key, str.encode(msg)))


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(map_handler=map_handler, reduce_handler=reduce_handler)
    grpc_server.start()
