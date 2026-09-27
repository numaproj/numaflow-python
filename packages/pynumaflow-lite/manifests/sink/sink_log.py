import asyncio
import logging
from collections.abc import AsyncIterable

from pynumaflow_lite.sinker import Datum, Response, SinkAsyncServer, Sinker

# Configure logging
logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)


class SimpleLogSink(Sinker):
    """
    Simple log sink that logs each message and returns success responses.
    """

    async def handler(self, datums: AsyncIterable[Datum]) -> list[Response]:
        responses = []
        async for msg in datums:
            _LOGGER.info("User Defined Sink: %s", msg.value.decode("utf-8"))
            responses.append(Response.success(msg.id))
            # if we are not able to write to sink and if we have a fallback sink configured
            # we can use Response.fallback(msg.id) to write the message to fallback sink
        return responses


async def main() -> None:
    print("Starting sink server")
    # `serve` returns when SIGINT or SIGTERM arrives.
    await SinkAsyncServer(SimpleLogSink()).serve()
    print("Sink server stopped")


if __name__ == "__main__":
    asyncio.run(main())
