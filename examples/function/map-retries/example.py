from typing import List

from pynumaflow.function import Messages, Message, Datum, Server


def my_handler(keys: List[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    strs = val.decode("utf-8").split(",")
    messages = Messages()
    if len(strs) == 0:
        messages.append(Message.to_drop())
        return messages
    for s in strs:
        messages.append(Message(str.encode(s)))
    return messages


if __name__ == "__main__":
    grpc_server = Server(map_handler=my_handler)
    grpc_server.start()
