import asyncio
import collections.abc
import logging
import signal
from typing import Awaitable, Callable

from pynumaflow_lite import sinker

# Configure logging
logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)


async def async_handler(datums: collections.abc.AsyncIterator[sinker.Datum]) -> sinker.Responses:
    """
    Simple log sink that logs each message and returns success responses.
    Also demonstrates reading metadata (read-only for sink).
    """
    responses = sinker.Responses()
    async for msg in datums:
        _LOGGER.info("User Defined Sink %s", msg.value.decode("utf-8"))

        # Read system metadata (read-only)
        _LOGGER.info("System metadata groups: %s", msg.system_metadata.groups())
        for group in msg.system_metadata.groups():
            for key in msg.system_metadata.keys(group):
                value = msg.system_metadata.value(group, key)
                _LOGGER.info("  System[%s][%s] = %s", group, key, value)

        # Read user metadata (read-only)
        _LOGGER.info("User metadata groups: %s", msg.user_metadata.groups())
        for group in msg.user_metadata.groups():
            for key in msg.user_metadata.keys(group):
                value = msg.user_metadata.value(group, key)
                _LOGGER.info("  User[%s][%s] = %s", group, key, value)

        responses.append(sinker.Response.as_success(msg.id))
        # if we are not able to write to sink and if we have a fallback sink configured
        # we can use Response.as_fallback(msg.id) to write the message to fallback sink
    return responses


async def start(f: Callable[[collections.abc.AsyncIterator[sinker.Datum]], Awaitable[sinker.Responses]]):
    sock_file = "/tmp/var/run/numaflow/sink.sock"
    server_info_file = "/tmp/var/run/numaflow/sinker-server-info"
    server = sinker.SinkAsyncServer(sock_file, server_info_file)

    # Register loop-level signal handlers to request graceful shutdown
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        pass

    try:
        await server.start(f)
        print("Shutting down gracefully...")
    except asyncio.CancelledError:
        try:
            server.stop()
        except Exception:
            pass
        return


if __name__ == "__main__":
    asyncio.run(start(async_handler))

