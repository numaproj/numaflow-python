import collections.abc
import logging

from pynumaflow_lite import sinker

# Configure logging
logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)


async def async_handler(
    datums: collections.abc.AsyncIterator[sinker.Datum],
) -> list[sinker.Response]:
    """
    Simple log sink that logs each message and returns success responses.
    Also demonstrates reading metadata (read-only for sink).
    """
    responses = []
    async for msg in datums:
        _LOGGER.info("User Defined Sink %s", msg.value.decode("utf-8"))

        # Read system metadata (read-only)
        _LOGGER.info("System metadata groups: %s", list(msg.system_metadata))
        for group, key_values in msg.system_metadata.items():
            for key, value in key_values.items():
                _LOGGER.info("  System[%s][%s] = %s", group, key, value)

        # Read user metadata (read-only)
        _LOGGER.info("User metadata groups: %s", list(msg.user_metadata))
        for group, key_values in msg.user_metadata.items():
            for key, value in key_values.items():
                _LOGGER.info("  User[%s][%s] = %s", group, key, value)

        responses.append(sinker.Response.success(msg.id))
        # if we are not able to write to sink and if we have a fallback sink configured
        # we can use Response.fallback(msg.id) to write the message to fallback sink
    return responses


if __name__ == "__main__":
    sinker.SinkAsyncServer(
        async_handler,
        sock_file="/tmp/var/run/numaflow/sink.sock",
        server_info_file="/tmp/var/run/numaflow/sinker-server-info",
    ).run()
