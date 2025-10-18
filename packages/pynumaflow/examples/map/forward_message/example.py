import os

from pynumaflow.mapper import Messages, Message, Datum, MapServer, Mapper


class MessageForwarder(Mapper):
    """
    This is a class that inherits from the Mapper class.
    It implements the handler method that is called for each datum.
    """

    def handler(self, keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        return Messages(Message(value=val, keys=keys))


def my_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    return Messages(Message(value=val, keys=keys))


if __name__ == "__main__":
    """
    Use the class based approach or function based handler
    based on the env variable
    Both can be used and passed directly to the server class
    """
    invoke = os.getenv("INVOKE", "func_handler")
    if invoke == "class":
        handler = MessageForwarder()
    else:
        handler = my_handler
    grpc_server = MapServer(handler)
    grpc_server.start()
