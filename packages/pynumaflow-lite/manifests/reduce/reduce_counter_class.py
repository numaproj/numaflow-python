import asyncio
import functools
from collections.abc import AsyncIterator

from pynumaflow_lite.reducer import Datum, Message, Metadata, ReduceAsyncServer, Reducer


class ReduceCounter(Reducer):
    """Counts the datums of each window. The server creates a new instance for each window."""

    def __init__(self, initial: int = 0) -> None:
        self.initial = initial

    async def handler(self, keys: list[str], datums: AsyncIterator[Datum], md: Metadata) -> list[Message]:
        counter = self.initial
        async for _ in datums:
            counter += 1
        iw = md.interval_window
        msg = f"counter:{counter} interval_window_start:{iw.start} interval_window_end:{iw.end}"
        return [Message(msg.encode(), keys=keys)]


async def main() -> None:
    # ReduceAsyncServer needs a factory: a callable that takes no arguments and
    # returns a new Reducer. The server calls it at the start of every window (once
    # for each set of keys), so each window starts with its own state.
    # functools.partial binds initial=100, so the server can call
    # ReduceCounter(initial=100) without arguments.
    # `serve` returns when SIGINT or SIGTERM arrives.
    await ReduceAsyncServer(functools.partial(ReduceCounter, initial=100)).serve()


if __name__ == "__main__":
    asyncio.run(main())
