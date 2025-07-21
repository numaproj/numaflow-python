import logging
import os
from collections.abc import AsyncIterable
from datetime import datetime

from pynumaflow import setup_logging
from pynumaflow.accumulator import Accumulator, AccumulatorAsyncServer
from pynumaflow.accumulator import (
    Message,
    Datum,
)
from pynumaflow.shared.asynciter import NonBlockingIterator

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)


class StreamSorter(Accumulator):
    def __init__(self):
        _LOGGER.info("StreamSorter initialized")
        self.latest_wm = datetime.fromtimestamp(-1)
        self.sorted_buffer: list[Datum] = []

    async def handler(
        self,
        datums: AsyncIterable[Datum],
        output: NonBlockingIterator,
    ):
        _LOGGER.info("StreamSorter handler started")
        async for datum in datums:
            _LOGGER.info(
                f"Received datum with event time: {datum.event_time}, "
                f"Current latest watermark: {self.latest_wm}, "
                f"Datum watermark: {datum.watermark}"
            )

            # If watermark has moved forward
            if datum.watermark and datum.watermark > self.latest_wm:
                self.latest_wm = datum.watermark
                await self.flush_buffer(output)

            self.insert_sorted(datum)

    def insert_sorted(self, datum: Datum):
        # Binary insert to keep sorted buffer in order
        left, right = 0, len(self.sorted_buffer)
        while left < right:
            mid = (left + right) // 2
            if self.sorted_buffer[mid].event_time > datum.event_time:
                right = mid
            else:
                left = mid + 1
        self.sorted_buffer.insert(left, datum)

    async def flush_buffer(self, output: NonBlockingIterator):
        _LOGGER.info(f"Watermark updated, flushing sortedBuffer: {self.latest_wm}")
        i = 0
        for datum in self.sorted_buffer:
            if datum.event_time > self.latest_wm:
                break
            await output.put(Message.from_datum(datum))
            _LOGGER.info(f"Sent datum with event time: {datum.event_time}")
            i += 1
        # Remove flushed items
        self.sorted_buffer = self.sorted_buffer[i:]


if __name__ == "__main__":
    invoke = os.getenv("INVOKE", "class")
    grpc_server = None
    if invoke == "class":
        # Here we are using the class instance as the accumulator_instance
        # which will be used to invoke the handler function.
        # We are passing the init_args for the class instance.
        grpc_server = AccumulatorAsyncServer(StreamSorter)
    grpc_server.start()
