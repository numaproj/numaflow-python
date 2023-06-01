from collections.abc import AsyncIterable

from pynumaflow.function import Messages, Message, Datum, Server


async def my_handler(keys: list[str], datum: Datum) -> AsyncIterable[Message]:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    strs = val.decode("utf-8").split(",")
    messages = Messages()
    if len(strs) == 0:
        yield Message.to_drop()
        return
    for s in strs:
        yield Message(str.encode(s))


if __name__ == "__main__":
    grpc_server = Server(map_handler=my_handler)
    grpc_server.start()
