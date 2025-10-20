import asyncio
import signal
from collections.abc import AsyncIterator

from pynumaflow_lite import mapstreamer
from pynumaflow_lite.mapstreamer import Message


async def async_handler(keys: list[str], datum: mapstreamer.Datum) -> AsyncIterator[Message]:
    """
    A handler that splits the input datum value into multiple strings by `,` separator and
    emits them as a stream.
    """
    parts = datum.value.decode("utf-8").split(",")
    if not parts:
        yield Message.to_drop()
        return
    for s in parts:
        yield Message(s.encode(), keys)


async def start(f: callable):
    sock_file = "/tmp/var/run/numaflow/mapstream.sock"
    server_info_file = "/tmp/var/run/numaflow/mapper-server-info"
    server = mapstreamer.MapStreamAsyncServer(sock_file, server_info_file)

    # Register loop-level signal handlers to request graceful shutdown
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        pass

    try:
        await server.start(f)
        print("Shutting down gracefully...")
    except asyncio.CancelledError:
        try:
            server.stop()
        except Exception:
            pass
        return


if __name__ == "__main__":
    asyncio.run(start(async_handler))

