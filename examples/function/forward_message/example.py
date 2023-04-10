from typing import List

from pynumaflow.function import Messages, Message, Datum, UserDefinedFunctionServicer


def my_handler(keys: List[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    messages.append(Message(keys, val))
    return messages


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(map_handler=my_handler)
    grpc_server.start()
