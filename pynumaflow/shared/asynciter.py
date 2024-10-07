import asyncio

from pynumaflow._constants import STREAM_EOF


class NonBlockingIterator:
    """An Async Interator backed by a queue"""

    __slots__ = "_queue"

    def __init__(self, size=0):
        self._queue = asyncio.Queue(maxsize=size)

    async def read_iterator(self):
        item = await self._queue.get()
        while True:
            if item == STREAM_EOF:
                break
            yield item
            item = await self._queue.get()

    async def put(self, item):
        await self._queue.put(item)
