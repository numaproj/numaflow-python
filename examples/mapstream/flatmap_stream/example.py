import os
from collections.abc import AsyncIterable
from pynumaflow.mapstreamer import Message, Datum, MapStreamAsyncServer, MapStreamer


class FlatMapStream(MapStreamer):
    async def handler(self, keys: list[str], datum: Datum) -> AsyncIterable[Message]:
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
    invoke = os.getenv("INVOKE", "func_handler")
    if invoke == "class":
        handler = FlatMapStream()
    else:
        handler = map_stream_handler
    grpc_server = MapStreamAsyncServer(handler)
    grpc_server.start()
