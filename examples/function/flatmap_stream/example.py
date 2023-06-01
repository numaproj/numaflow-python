import aiorun
from collections.abc import AsyncIterable

from pynumaflow.function import Message, Datum, AsyncServer


async def map_stream_handler(_: list[str], datum: Datum) -> AsyncIterable[Message]:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    strs = val.decode("utf-8").split(",")

    if len(strs) == 0:
        yield Message.to_drop()
        return
    for s in strs:
        yield Message(str.encode(s))


if __name__ == "__main__":
    grpc_server = AsyncServer(map_stream_handler=map_stream_handler)
    aiorun.run(grpc_server.start())
