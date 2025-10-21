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
