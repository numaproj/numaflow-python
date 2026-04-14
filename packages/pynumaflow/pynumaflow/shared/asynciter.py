import asyncio
from typing import Generic, TypeVar
from collections.abc import AsyncIterator

from pynumaflow._constants import STREAM_EOF

T = TypeVar("T")


class NonBlockingIterator(Generic[T]):
    """An Async Interator backed by a queue"""

    __slots__ = "_queue"

    def __init__(self, size: int = 0) -> None:
        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=size)

    async def read_iterator(self) -> AsyncIterator[T]:
        item = await self._queue.get()
        while True:
            if item == STREAM_EOF:
                break
            yield item
            item = await self._queue.get()

    async def put(self, item: T) -> None:
        await self._queue.put(item)
        # Yield to the event loop after each put.  The underlying
        # asyncio.Queue is unbounded (maxsize=0), so Queue.put() never
        # actually suspends — it calls sync put_nowait() under the hood.
        # If the UDF async generator yields messages via a sync for-loop
        # (no await between yields), the event loop is starved and
        # consumer tasks (including gRPC streaming) cannot make progress
        # until the generator completes.  The sleep(0) ensures the event
        # loop gets a turn after every put regardless of the caller's code.
        # See: https://github.com/numaproj/numaflow-python/issues/350
        await asyncio.sleep(0)
