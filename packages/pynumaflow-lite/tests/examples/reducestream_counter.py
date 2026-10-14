import asyncio
import functools
from collections.abc import AsyncIterator

from pynumaflow_lite.reducestreamer import Datum, Message, Metadata, ReduceStreamAsyncServer, ReduceStreamer


class ReduceStreamCounter(ReduceStreamer):
    """
    A reduce streaming counter that emits intermediate results.

    This test implementation counts datums and yields a message for every
    datum received, demonstrating the streaming capability.
    """

    def __init__(self, initial: int = 0) -> None:
        self.counter = initial

    async def handler(self, keys: list[str], datums: AsyncIterator[Datum], md: Metadata) -> AsyncIterator[Message]:
        """
        Process datums and yield messages incrementally.

        For testing purposes, we yield a message for each datum received.
        """
        iw = md.interval_window
        async for _ in datums:
            self.counter += 1
            # Yield a message for each datum (streaming behavior)
            msg = f"counter:{self.counter} interval_window_start:{iw.start} interval_window_end:{iw.end}"
            yield Message(msg.encode(), keys=keys)


async def main() -> None:
    await ReduceStreamAsyncServer(
        functools.partial(ReduceStreamCounter, initial=0),
        sock_file="/tmp/var/run/numaflow/reducestream.sock",
        server_info_file="/tmp/var/run/numaflow/reducestreamer-server-info",
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())
