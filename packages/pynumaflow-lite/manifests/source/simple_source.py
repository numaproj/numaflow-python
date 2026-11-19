import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from pynumaflow_lite.sourcer import Message, NackOffset, Offset, ReadRequest, SourceAsyncServer, Sourcer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleSource(Sourcer):
    """
    A source that generates messages with incrementing numbers.

    It keeps the offsets that are read but not acked yet. A nacked offset is
    read again before any new message.
    """

    def __init__(self, partition_id: int = 0) -> None:
        self.partition_id = partition_id
        self.next_id = 0
        self.unacked: set[Offset] = set()
        self.redeliver: deque[Offset] = deque()

    def _next_offset(self) -> Offset:
        if self.redeliver:
            return self.redeliver.popleft()
        offset = Offset(str(self.next_id).encode(), self.partition_id)
        self.next_id += 1
        return offset

    async def read(self, request: ReadRequest) -> AsyncIterator[Message]:
        logger.info("Read request: %r", request)
        for _ in range(request.num_records):
            offset = self._next_offset()
            self.unacked.add(offset)
            yield Message(
                f"message-{offset.value.decode()}".encode(),
                offset,
                datetime.now(UTC),
                keys=["key1"],
                headers={"source": "simple"},
                user_metadata={
                    "source_info": {
                        "source_name": b"simple_source",
                        "message_id": offset.value,
                        "partition": str(offset.partition_id).encode(),
                    }
                },
            )

    async def ack(self, offsets: list[Offset]) -> None:
        logger.info("Acknowledging %d offsets", len(offsets))
        self.unacked.difference_update(offsets)

    async def nack(self, offsets: list[NackOffset]) -> None:
        logger.info("Negatively acknowledging %d offsets", len(offsets))
        for nack_offset in offsets:
            logger.warning("Nacked %r", nack_offset)
            self.unacked.discard(nack_offset.offset)
            self.redeliver.append(nack_offset.offset)

    async def pending(self) -> int | None:
        # Only the nacked messages wait to be read: new messages are made on demand.
        return len(self.redeliver)

    async def active_partitions(self) -> list[int] | None:
        return [self.partition_id]


async def main() -> None:
    await SourceAsyncServer(SimpleSource()).serve()


if __name__ == "__main__":
    asyncio.run(main())
