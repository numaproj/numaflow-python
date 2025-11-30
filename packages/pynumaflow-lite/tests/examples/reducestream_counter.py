import asyncio
import signal
from collections.abc import AsyncIterable, AsyncIterator

from pynumaflow_lite import reducestreamer


class ReduceStreamCounter(reducestreamer.ReduceStreamer):
    """
    A reduce streaming counter that emits intermediate results.
    
    This test implementation counts datums and yields a message for every
    datum received, demonstrating the streaming capability.
    """
    
    def __init__(self, initial: int = 0) -> None:
        self.counter = initial

    async def handler(
        self,
        keys: list[str],
        datums: AsyncIterable[reducestreamer.Datum],
        md: reducestreamer.Metadata,
    ) -> AsyncIterator[reducestreamer.Message]:
        """
        Process datums and yield messages incrementally.
        
        For testing purposes, we yield a message for each datum received.
        """
        iw = md.interval_window
        self.counter = 0
        
        async for _ in datums:
            self.counter += 1
            # Yield a message for each datum (streaming behavior)
            msg = (
                f"counter:{self.counter} "
                f"interval_window_start:{iw.start} "
                f"interval_window_end:{iw.end}"
            ).encode()
            yield reducestreamer.Message(msg, keys=keys)


# Optional: ensure default signal handlers are in place so asyncio.run can handle them cleanly.
signal.signal(signal.SIGINT, signal.default_int_handler)
try:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
except AttributeError:
    pass


async def start(creator: type, init_args: tuple):
    """Start the reduce stream server."""
    sock_file = "/tmp/var/run/numaflow/reducestream.sock"
    server_info_file = "/tmp/var/run/numaflow/reducestreamer-server-info"
    server = reducestreamer.ReduceStreamAsyncServer(sock_file, server_info_file)

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
    asyncio.run(start(ReduceStreamCounter, (0,)))

