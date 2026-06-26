import asyncio
import signal
from collections.abc import AsyncIterable, Awaitable, Callable

from pynumaflow_lite import reducer


async def reduce_handler(
    keys: list[str], datums: AsyncIterable[reducer.Datum], md: reducer.Metadata
) -> reducer.Messages:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = f"counter:{counter} interval_window_start:{interval_window.start} interval_window_end:{interval_window.end}"
    out = reducer.Messages()
    out.append(reducer.Message(str.encode(msg), keys=keys))
    return out


async def start(
    handler: Callable[
        [list[str], AsyncIterable[reducer.Datum], reducer.Metadata],
        Awaitable[reducer.Messages],
    ],
):
    sock_file = "/tmp/var/run/numaflow/reduce.sock"
    server_info_file = "/tmp/var/run/numaflow/reducer-server-info"
    server = reducer.ReduceAsyncServer(sock_file, server_info_file)

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        pass

    try:
        await server.start(handler)
        print("Shutting down gracefully...")
    except asyncio.CancelledError:
        server.stop()
        return


if __name__ == "__main__":
    asyncio.run(start(reduce_handler))
