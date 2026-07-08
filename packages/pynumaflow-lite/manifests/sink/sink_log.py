import logging
from collections.abc import AsyncIterable

from pynumaflow_lite import sinker
from pynumaflow_lite.sinker import Sinker

# Configure logging
logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)


class SimpleLogSink(Sinker):
    """
    Simple log sink that logs each message and returns success responses.
    """

    async def handler(self, datums: AsyncIterable[sinker.Datum]) -> list[sinker.Response]:
        responses = []
        async for msg in datums:
            _LOGGER.info("User Defined Sink: %s", msg.value.decode("utf-8"))
            responses.append(sinker.Response.success(msg.id))
            # if we are not able to write to sink and if we have a fallback sink configured
            # we can use Response.fallback(msg.id) to write the message to fallback sink
        return responses


if __name__ == "__main__":
    sinker.SinkAsyncServer(SimpleLogSink()).run()
