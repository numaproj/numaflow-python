from queue import Queue

from pynumaflow._constants import STREAM_EOF


class SyncIterator:
    """A Sync Interator backed by a queue"""

    __slots__ = "_queue"

    def __init__(self, size=0):
        self._queue = Queue(maxsize=size)

    def read_iterator(self):
        item = self._queue.get()
        while True:
            if item == STREAM_EOF:
                break
            yield item
            item = self._queue.get()

    def put(self, item):
        self._queue.put(item)
