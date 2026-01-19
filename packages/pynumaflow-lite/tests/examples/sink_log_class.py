import asyncio
import logging
import signal

from pynumaflow_lite import sinker
from pynumaflow_lite._sink_dtypes import Sinker
from collections.abc import AsyncIterable

# Configure logging
logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)


class SimpleLogSink(Sinker):
    """
    Simple log sink that logs each message and returns success responses.
    This is the class-based approach matching the user's example.
    Also demonstrates reading metadata (read-only for sink).
    """

    async def handler(self, datums: AsyncIterable[sinker.Datum]) -> sinker.Responses:
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


async def start():
    sock_file = "/tmp/var/run/numaflow/sink.sock"
    server_info_file = "/tmp/var/run/numaflow/sinker-server-info"
    server = sinker.SinkAsyncServer(sock_file, server_info_file)

    # Create an instance of the sink handler
    handler = SimpleLogSink()

    # Register loop-level signal handlers to request graceful shutdown
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: server.stop())
        loop.add_signal_handler(signal.SIGTERM, lambda: server.stop())
    except (NotImplementedError, RuntimeError):
        pass

    try:
        await server.start(handler)
        print("Shutting down gracefully...")
    except asyncio.CancelledError:
        try:
            server.stop()
        except Exception:
            pass
        return


if __name__ == "__main__":
    asyncio.run(start())

