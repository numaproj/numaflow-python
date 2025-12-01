"""
Reduce Streaming Counter Example

This example demonstrates how to use ReduceStreamer to emit intermediate results
as data arrives, rather than waiting until all data is received.

The counter increments for each datum and emits a message every 10 items,
plus a final message at the end.
"""
import asyncio
import signal
from collections.abc import AsyncIterable, AsyncIterator

from pynumaflow_lite import reducestreamer


class ReduceCounter(reducestreamer.ReduceStreamer):
    """
    A reduce streaming counter that emits intermediate results.
    
    This demonstrates the key difference from regular Reducer:
    - Regular Reducer: waits for all data, then returns Messages
    - ReduceStreamer: yields Message objects incrementally as an async iterator
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
        
        Args:
            keys: List of keys for this window
            datums: Async iterable of incoming data
            md: Metadata containing window information
            
        Yields:
            Message objects to send to the next vertex
        """
        iw = md.interval_window
        print(f"Handler started for keys={keys}, window=[{iw.start}, {iw.end}]")
        
        async for _ in datums:
            self.counter += 1
            
            # Emit intermediate result every 10 items
            if self.counter % 10 == 0:
                msg = (
                    f"counter:{self.counter} "
                    f"interval_window_start:{iw.start} "
                    f"interval_window_end:{iw.end}"
                ).encode()
                print(f"Yielding intermediate result: counter={self.counter}")
                # Early release of data - this is the key feature of reduce streaming!
                yield reducestreamer.Message(msg, keys=keys)
        
        # Emit final result
        msg = (
            f"counter:{self.counter} (FINAL) "
            f"interval_window_start:{iw.start} "
            f"interval_window_end:{iw.end}"
        ).encode()
        print(f"Yielding final result: counter={self.counter}")
        yield reducestreamer.Message(msg, keys=keys)


# Optional: ensure default signal handlers are in place so asyncio.run can handle them cleanly.
signal.signal(signal.SIGINT, signal.default_int_handler)
try:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
except AttributeError:
    pass


async def start(creator: type, init_args: tuple):
    """Start the reduce stream server."""
    sock_file = "/var/run/numaflow/reducestream.sock"
    server_info_file = "/var/run/numaflow/reducestreamer-server-info"
    server = reducestreamer.ReduceStreamAsyncServer(sock_file, server_info_file)

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        pass

    try:
        print("Starting Reduce Stream Counter Server...")
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

