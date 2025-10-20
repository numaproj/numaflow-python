#!/usr/bin/env python3
"""
Example session reduce handler that counts messages per session.

This demonstrates:
- session_reduce: counts incoming messages and yields the count
- accumulator: returns current count as bytes
- merge_accumulator: merges counts from another session
"""

import asyncio
import signal
from collections.abc import AsyncIterable, AsyncIterator

from pynumaflow_lite import session_reducer


class SessionReduceCounter(session_reducer.SessionReducer):
    """
    A session reducer that counts all messages in a session.
    When sessions are merged, the counts are added together.
    """

    def __init__(self, initial: int = 0) -> None:
        self.counter = initial

    async def session_reduce(
            self, keys: list[str], datums: AsyncIterable[session_reducer.Datum]
    ) -> AsyncIterator[session_reducer.Message]:
        """
        Count all incoming messages in this session and yield the count.
        """
        # Count all incoming messages in this session
        async for _ in datums:
            self.counter += 1

        # Emit the current count as bytes with the same keys
        yield session_reducer.Message(str(self.counter).encode(), keys)

    async def accumulator(self) -> bytes:
        """
        Return current count as bytes for merging with other sessions.
        """
        return str(self.counter).encode()

    async def merge_accumulator(self, accumulator: bytes) -> None:
        """
        Parse serialized accumulator and add to our count.
        """
        try:
            self.counter += int(accumulator.decode("utf-8"), 10)
        except Exception as e:
            import sys

            print(f"Failed to parse accumulator value: {e}", file=sys.stderr)


async def main():
    """
    Start the session reduce server.
    """
    sock_file = "/tmp/var/run/numaflow/sessionreduce.sock"
    server_info_file = "/tmp/var/run/numaflow/sessionreducer-server-info"
    server = session_reducer.SessionReduceAsyncServer(sock_file, server_info_file)

    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        pass

    try:
        await server.start(SessionReduceCounter)
        print("Shutting down gracefully...")
    except asyncio.CancelledError:
        try:
            server.stop()
        except Exception:
            pass
        return


# Optional: ensure default signal handlers are in place so asyncio.run can handle them cleanly.
signal.signal(signal.SIGINT, signal.default_int_handler)
try:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
except AttributeError:
    pass

if __name__ == "__main__":
    asyncio.run(main())
