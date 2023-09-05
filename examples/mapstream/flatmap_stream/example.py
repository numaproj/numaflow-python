import aiorun
from collections.abc import AsyncIterable

from pynumaflow.mapstream import Message, Datum, AsyncMapStreamer


async def map_stream_handler(_: list[str], datum: Datum) -> AsyncIterable[Message]:
    """
    A handler that splits the input datum value into multiple strings by `,` separator and
    emits them as a stream.
    """
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
    grpc_server = AsyncMapStreamer(map_stream_handler=map_stream_handler)
    aiorun.run(grpc_server.start())
