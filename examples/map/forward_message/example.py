import os

from pynumaflow.mapper import Messages, Message, Datum, MapServer, Mapper


class Example(Mapper):
    def handler(self, keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        messages = Messages()
        messages.append(Message(value=val, keys=keys))
        return messages


def my_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    messages.append(Message(value=val, keys=keys))
    return messages


if __name__ == "__main__":
    invoke = os.getenv("INVOKE", "handler")
    # Use the class based approach or function based handler
    # based on the env variable
    # Both can be used and passed directly to the server class
    if invoke == "class":
        handler = Example()
    else:
        handler = my_handler
    grpc_server = MapServer(handler)
    grpc_server.start()
