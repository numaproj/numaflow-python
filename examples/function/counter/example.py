from typing import Iterator
from pynumaflow.function import Messages, Message, \
    Datum, Metadata, UserDefinedFunctionServicer


def map_handler(key: str, datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages(Message.to_vtx(key, val))
    return messages


def reduce_handler(key: str, datums: Iterator[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    counter = 0
    for _ in datums:
        counter = counter + 1
    msg = "counter:%s interval_window_start:%s interval_window_end:%s" % (
        counter,
        interval_window.start,
        interval_window.end,
    )
    messages = Messages()
    messages.append(Message.to_vtx(key, str.encode(msg)))
    return messages


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(
        map_handler=map_handler,
        reduce_handler=reduce_handler,
    )
    grpc_server.start()
