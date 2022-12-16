from typing import Iterator
from pynumaflow.function import Messages, Message, Datum, Metadata, UserDefinedFunctionServicer


def my_handler(key: str, datums: Iterator[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    print("Interval window:", interval_window)
    counter = 0
    for datum in datums:
        print("datum:", datum)
        counter = counter + 1
    messages = Messages()
    messages.append(Message.to_vtx(key, str.encode(str(counter))))
    return messages


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(reduce_handler=my_handler)
    grpc_server.start()
