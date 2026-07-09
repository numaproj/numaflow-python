from collections.abc import AsyncIterable

from pynumaflow_lite import mapstreamer
from pynumaflow_lite.mapstreamer import Message


async def async_handler(datum: mapstreamer.Datum) -> AsyncIterable[Message]:
    """
    A handler that splits the input datum value into multiple strings by `,` separator and
    emits them as a stream.
    """
    parts = datum.value.decode("utf-8").split(",")
    if not parts:
        yield Message.to_drop()
        return
    for s in parts:
        yield Message(s.encode(), keys=datum.keys)


if __name__ == "__main__":
    mapstreamer.MapStreamAsyncServer(
        async_handler,
        sock_file="/tmp/var/run/numaflow/mapstream.sock",
        server_info_file="/tmp/var/run/numaflow/mapper-server-info",
    ).run()
