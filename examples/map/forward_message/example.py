import os

from pynumaflow.mapper import Messages, Message, Datum, MapServer, Mapper


class Example(Mapper):
    """
    This is a class that inherits from the Mapper class.
    It implements the handler method that is called for each datum.
    """

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
    """
    Use the class based approach or function based handler
    based on the env variable
    Both can be used and passed directly to the server class
    """
    invoke = os.getenv("INVOKE", "handler")
    if invoke == "class":
        handler = Example()
    else:
        handler = my_handler
    grpc_server = MapServer(handler)
    grpc_server.start()
