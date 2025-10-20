import asyncio
import signal
from collections.abc import AsyncIterable

from pynumaflow_lite import reducer


class ReduceCounter(reducer.Reducer):
    def __init__(self, initial: int = 0) -> None:
        self.counter = initial

    async def handler(
        self, keys: list[str], datums: AsyncIterable[reducer.Datum], md: reducer.Metadata
    ) -> reducer.Messages:
        iw = md.interval_window
        self.counter = 0
        async for _ in datums:
            self.counter += 1
        msg = (
            f"counter:{self.counter} interval_window_start:{iw.start} interval_window_end:{iw.end}"
        ).encode()
        out = reducer.Messages()
        out.append(reducer.Message(msg, keys))
        return out


# Optional: ensure default signal handlers are in place so asyncio.run can handle them cleanly.
signal.signal(signal.SIGINT, signal.default_int_handler)
try:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
except AttributeError:
    pass


async def start(creator: type, init_args: tuple):
    sock_file = "/var/run/numaflow/reduce.sock"
    server_info_file = "/var/run/numaflow/reducer-server-info"
    server = reducer.ReduceAsyncServer(sock_file, server_info_file)

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        pass

    try:
        await server.start(creator, init_args)
        print("Shutting down gracefully...")
    except asyncio.CancelledError:
        try:
            server.stop()
        except Exception:
            pass
        return


if __name__ == "__main__":
    asyncio.run(start(ReduceCounter, (0,)))

