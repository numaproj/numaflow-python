from typing import Iterator
from pynumaflow.function import Messages, Message, Datum, Metadata, UserDefinedFunctionServicer


def reduce_handler(key: str, datums: Iterator[Datum], md: Metadata) -> Messages:
    print("Called reduce handler")
    interval_window = md.interval_window
    print("start", interval_window.start, "end", interval_window.end)
    counter = 0
    for d in datums:
        print("Got datum", d.value)
        counter += 1
    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    return Messages(Message.to_vtx(key, str.encode(msg)))


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(reduce_handler=reduce_handler)
    grpc_server.start()
