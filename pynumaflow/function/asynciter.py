import asyncio


class NonBlockingIterator:
    """An Async Interator backed by a queue"""

    __slots__ = "_queue"

    def __init__(self):
        self._queue = asyncio.Queue()

    async def read_iterator(self):
        item = await self._queue.get()
        while True:
            if item == "EOF":
                break
            yield item
            item = await self._queue.get()

    async def put(self, item):
        await self._queue.put(item)
