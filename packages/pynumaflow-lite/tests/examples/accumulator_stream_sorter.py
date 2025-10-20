"""
Stream sorter accumulator example.

This accumulator buffers incoming data and sorts it by event time,
flushing sorted data when the watermark advances.
"""
import asyncio
from datetime import datetime
from typing import AsyncIterator

from pynumaflow_lite.accumulator import Datum, Message, AccumulatorAsyncServer, Accumulator


class StreamSorter(Accumulator):
    """
    A stream sorter that buffers and sorts data by event time,
    flushing when watermark advances.
    """

    def __init__(self):
        from datetime import timezone
        # Initialize with a very old timestamp (timezone-aware)
        self.latest_wm = datetime.fromtimestamp(-1, tz=timezone.utc)
        self.sorted_buffer: list[Datum] = []
        print("StreamSorter initialized")

    async def handler(self, datums: AsyncIterator[Datum]) -> AsyncIterator[Message]:
        """
        Buffer and sort datums, yielding sorted messages when watermark advances.
        """
        print("Handler started, waiting for datums...")
        datum_count = 0

        async for datum in datums:
            datum_count += 1
            print(f"Received datum #{datum_count}: event_time={datum.event_time}, "
                  f"watermark={datum.watermark}, value={datum.value}")

            # If watermark has moved forward
            if datum.watermark and datum.watermark > self.latest_wm:
                old_wm = self.latest_wm
                self.latest_wm = datum.watermark
                print(f"Watermark advanced from {old_wm} to {self.latest_wm}")

                # Flush buffer
                flushed = 0
                async for msg in self.flush_buffer():
                    flushed += 1
                    yield msg

                if flushed > 0:
                    print(f"Flushed {flushed} messages from buffer")

            # Insert into sorted buffer
            self.insert_sorted(datum)
            print(f"Buffer size: {len(self.sorted_buffer)}")

        print(f"Handler finished. Total datums processed: {datum_count}")
        print(f"Remaining in buffer: {len(self.sorted_buffer)}")

        # Flush any remaining items in the buffer at the end
        if self.sorted_buffer:
            print("Flushing remaining buffer at end...")
            for datum in self.sorted_buffer:
                print(f"  Flushing: event_time={datum.event_time}, value={datum.value}")
                # Use Message.from_datum to preserve all metadata
                yield Message.from_datum(datum)
            self.sorted_buffer = []

    def insert_sorted(self, datum: Datum):
        """Binary insert to keep sorted buffer in order by event_time."""
        left, right = 0, len(self.sorted_buffer)
        while left < right:
            mid = (left + right) // 2
            if self.sorted_buffer[mid].event_time > datum.event_time:
                right = mid
            else:
                left = mid + 1
        self.sorted_buffer.insert(left, datum)

    async def flush_buffer(self) -> AsyncIterator[Message]:
        """Flush all items from buffer that are before or at the watermark."""
        i = 0
        for datum in self.sorted_buffer:
            if datum.event_time > self.latest_wm:
                break
            print(f"  Flushing: event_time={datum.event_time}, value={datum.value}")
            # Use Message.from_datum to preserve all metadata (id, headers, event_time, watermark)
            yield Message.from_datum(datum)
            i += 1

        # Remove flushed items
        self.sorted_buffer = self.sorted_buffer[i:]


async def main():
    """
    Start the accumulator server.
    """
    import signal

    sock_file = "/tmp/var/run/numaflow/accumulator.sock"
    server_info_file = "/tmp/var/run/numaflow/accumulator-server-info"
    server = AccumulatorAsyncServer(sock_file, server_info_file)

    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        pass

    try:
        print("Starting Stream Sorter Accumulator Server...")
        await server.start(StreamSorter)
        print("Shutting down gracefully...")
    except asyncio.CancelledError:
        try:
            server.stop()
        except Exception:
            pass
        return


# Optional: ensure default signal handlers are in place so asyncio.run can handle them cleanly.
import signal
signal.signal(signal.SIGINT, signal.default_int_handler)
try:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
except AttributeError:
    pass

if __name__ == "__main__":
    asyncio.run(main())
