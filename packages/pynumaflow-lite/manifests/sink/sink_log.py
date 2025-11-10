import asyncio
import collections
import logging
import signal
from collections.abc import AsyncIterable

from pynumaflow_lite import sinker
from pynumaflow_lite._sink_dtypes import Sinker

# Configure logging
logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)


class SimpleLogSink(Sinker):
    """
    Simple log sink that logs each message and returns success responses.
    """

    async def handler(self, datums: AsyncIterable[sinker.Datum]) -> sinker.Responses:
        responses = sinker.Responses()
        async for msg in datums:
            _LOGGER.info("User Defined Sink: %s", msg.value.decode("utf-8"))
            responses.append(sinker.Response.as_success(msg.id))
            # if we are not able to write to sink and if we have a fallback sink configured
            # we can use Response.as_fallback(msg.id) to write the message to fallback sink
        return responses


# Optional: ensure default signal handlers are in place so asyncio.run can handle them cleanly.
signal.signal(signal.SIGINT, signal.default_int_handler)
try:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
except AttributeError:
    pass


async def start(f: collections.abc.Callable):
    server = sinker.SinkAsyncServer()

    # Register loop-level signal handlers so we control shutdown and avoid asyncio.run
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
    async_handler = SimpleLogSink()
    asyncio.run(start(async_handler))

