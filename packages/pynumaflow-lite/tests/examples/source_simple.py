import asyncio
import logging
import signal
from datetime import datetime, timezone
from collections.abc import AsyncIterator

from pynumaflow_lite import sourcer
from pynumaflow_lite._source_dtypes import Sourcer

# Configure logging
logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)


class SimpleSource(Sourcer):
    """
    Simple source that generates messages with incrementing numbers.
    This is the class-based approach matching the user's example.
    """

    def __init__(self):
        self.counter = 0
        self.partition_idx = 0

    async def read_handler(self, datum: sourcer.ReadRequest) -> AsyncIterator[sourcer.Message]:
        """
        The simple source generates messages with incrementing numbers.
        Also demonstrates creating user metadata (source is origin, so only user metadata).
        """
        _LOGGER.info(f"Read request: num_records={datum.num_records}, timeout_ms={datum.timeout_ms}")

        # Generate the requested number of messages
        for i in range(datum.num_records):
            # Create message payload
            payload = f"message-{self.counter}".encode("utf-8")

            # Create offset
            offset = sourcer.Offset(
                offset=str(self.counter).encode("utf-8"),
                partition_id=self.partition_idx
            )

            # Create user metadata for the message
            user_metadata = sourcer.UserMetadata()
            user_metadata.create_group("source_info")
            user_metadata.add_kv("source_info", "source_name", b"simple_source")
            user_metadata.add_kv("source_info", "message_id", str(self.counter).encode())
            user_metadata.add_kv("source_info", "partition", str(self.partition_idx).encode())

            # Create message
            message = sourcer.Message(
                payload=payload,
                offset=offset,
                event_time=datetime.now(timezone.utc),
                keys=["key1"],
                headers={"source": "simple"},
                user_metadata=user_metadata
            )

            _LOGGER.info(f"Generated message: {self.counter}")
            self.counter += 1

            yield message

            # Small delay to simulate real source
            await asyncio.sleep(0.1)

    async def ack_handler(self, request: sourcer.AckRequest) -> None:
        """
        The simple source acknowledges the offsets.
        """
        _LOGGER.info(f"Acknowledging {len(request.offsets)} offsets")
        for offset in request.offsets:
            _LOGGER.debug(f"Acked offset: {offset.offset.decode('utf-8')}, partition: {offset.partition_id}")

    async def nack_handler(self, request: sourcer.NackRequest) -> None:
        """
        The simple source negatively acknowledges the offsets.
        """
        _LOGGER.info(f"Negatively acknowledging {len(request.offsets)} offsets")
        for offset in request.offsets:
            _LOGGER.warning(f"Nacked offset: {offset.offset.decode('utf-8')}, partition: {offset.partition_id}")

    async def pending_handler(self) -> sourcer.PendingResponse:
        """
        The simple source always returns zero to indicate there is no pending record.
        """
        return sourcer.PendingResponse(count=0)

    async def partitions_handler(self) -> sourcer.PartitionsResponse:
        """
        The simple source always returns default partitions.
        """
        return sourcer.PartitionsResponse(partitions=[self.partition_idx])


async def start():
    sock_file = "/tmp/var/run/numaflow/source.sock"
    server_info_file = "/tmp/var/run/numaflow/sourcer-server-info"
    server = sourcer.SourceAsyncServer(sock_file, server_info_file)

    # Create an instance of the source handler
    handler = SimpleSource()

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

