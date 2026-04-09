import os

from pynumaflow.mapper import Messages, Message, Datum, MapAsyncServer, Mapper


class AsyncMessageForwarder(Mapper):
    """
    This is a class that inherits from the Mapper class.
    It implements the handler method as an async function.
    """

    async def handler(self, keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        return Messages(Message(value=val, keys=keys))


async def my_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    return Messages(Message(value=val, keys=keys))


if __name__ == "__main__":
    """
    Use the class based approach or function based handler
    based on the env variable.
    Both can be used and passed directly to the server class.
    """
    invoke = os.getenv("INVOKE", "func_handler")
    if invoke == "class":
        handler = AsyncMessageForwarder()
    else:
        handler = my_handler
    grpc_server = MapAsyncServer(handler)
    grpc_server.start()
