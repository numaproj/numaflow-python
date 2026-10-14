"""
Reduce Streaming Counter Example

This example demonstrates how to use ReduceStreamer to emit intermediate results
as data arrives, rather than waiting until all data is received.

The counter increments for each datum and emits a message every 10 items,
plus a final message at the end.
"""

import asyncio
import functools
from collections.abc import AsyncIterator

from pynumaflow_lite.reducestreamer import Datum, Message, Metadata, ReduceStreamAsyncServer, ReduceStreamer


class ReduceCounter(ReduceStreamer):
    """
    A reduce streaming counter that emits intermediate results.

    This demonstrates the key difference from regular Reducer:
    - Regular Reducer: waits for all data, then returns list[Message]
    - ReduceStreamer: yields Message objects incrementally as an async iterator
    """

    def __init__(self, initial: int = 0) -> None:
        self.counter = initial

    async def handler(self, keys: list[str], datums: AsyncIterator[Datum], md: Metadata) -> AsyncIterator[Message]:
        """
        Process datums and yield messages incrementally.

        Args:
            keys: List of keys for this window
            datums: Async iterator of incoming data
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
                msg = f"counter:{self.counter} interval_window_start:{iw.start} interval_window_end:{iw.end}"
                print(f"Yielding intermediate result: counter={self.counter}")
                # Early release of data - this is the key feature of reduce streaming!
                yield Message(msg.encode(), keys=keys)

        # Emit final result
        msg = f"counter:{self.counter} (FINAL) interval_window_start:{iw.start} interval_window_end:{iw.end}"
        print(f"Yielding final result: counter={self.counter}")
        yield Message(msg.encode(), keys=keys)


async def main() -> None:
    # ReduceStreamAsyncServer needs a factory: a callable that takes no arguments
    # and returns a new ReduceStreamer. The server calls it at the start of every
    # window (once for each set of keys), so each window starts with its own state.
    # functools.partial binds initial=0, so the server can call
    # ReduceCounter(initial=0) without arguments.
    # `serve` returns when SIGINT or SIGTERM arrives.
    print("Starting Reduce Stream Counter Server...")
    await ReduceStreamAsyncServer(functools.partial(ReduceCounter, initial=0)).serve()


if __name__ == "__main__":
    asyncio.run(main())
