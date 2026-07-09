from collections.abc import AsyncIterable

from pynumaflow_lite import mapstreamer
from pynumaflow_lite.mapstreamer import Message


class SimpleStreamCat(mapstreamer.MapStreamer):
    async def handler(self, datum: mapstreamer.Datum) -> AsyncIterable[Message]:
        parts = datum.value.decode("utf-8").split(",")
        if not parts:
            yield Message.to_drop()
            return
        for s in parts:
            yield Message(s.encode(), keys=datum.keys)


if __name__ == "__main__":
    mapstreamer.MapStreamAsyncServer(SimpleStreamCat()).run()
