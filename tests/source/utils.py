from collections.abc import Iterable

from pynumaflow.shared.asynciter import NonBlockingIterator

from pynumaflow.sourcer import ReadRequest, Message
from pynumaflow.sourcer._dtypes import (
    AckRequest,
    PendingResponse,
    Offset,
    PartitionsResponse,
    Sourcer,
)
from pynumaflow.proto.sourcer import source_pb2
from tests.testing_utils import mock_event_time


def mock_offset() -> Offset:
    return Offset(b"offset_mock", 10)


def mock_partitions() -> list[int]:
    return [1, 2, 3]


class AsyncSource(Sourcer):
    async def read_handler(self, datum: ReadRequest, output: NonBlockingIterator):
        payload = b"payload:test_mock_message"
        keys = ["test_key"]
        offset = mock_offset()
        event_time = mock_event_time()
        for i in range(10):
            await output.put(
                Message(payload=payload, keys=keys, offset=offset, event_time=event_time)
            )

    async def ack_handler(self, ack_request: AckRequest):
        return

    async def pending_handler(self) -> PendingResponse:
        return PendingResponse(count=10)

    async def partitions_handler(self) -> PartitionsResponse:
        return PartitionsResponse(partitions=mock_partitions())


class SyncSource(Sourcer):
    def read_handler(self, datum: ReadRequest) -> Iterable[Message]:
        payload = b"payload:test_mock_message"
        keys = ["test_key"]
        offset = mock_offset()
        event_time = mock_event_time()
        for i in range(10):
            yield Message(payload=payload, keys=keys, offset=offset, event_time=event_time)

    def ack_handler(self, ack_request: AckRequest):
        return

    def pending_handler(self) -> PendingResponse:
        return PendingResponse(count=10)

    def partitions_handler(self) -> PartitionsResponse:
        return PartitionsResponse(partitions=mock_partitions())


def read_req_source_fn() -> ReadRequest:
    request = source_pb2.ReadRequest.Request(
        num_records=10,
        timeout_in_ms=1000,
    )
    return request


def ack_req_source_fn():
    msg = source_pb2.Offset(offset=mock_offset().offset, partition_id=mock_offset().partition_id)
    request = source_pb2.AckRequest.Request(offsets=[msg])
    return request


class AsyncSourceError(Sourcer):
    # This handler mimics the scenario where map stream UDF throws a runtime error.
    async def read_handler(self, datum: ReadRequest, output: NonBlockingIterator):
        payload = b"payload:test_mock_message"
        keys = ["test_key"]
        offset = mock_offset()
        event_time = mock_event_time()
        for i in range(datum.num_records):
            await output.put(
                Message(payload=payload, keys=keys, offset=offset, event_time=event_time)
            )
        raise RuntimeError("Got a runtime error from read handler.")

    async def ack_handler(self, ack_request: AckRequest):
        raise RuntimeError("Got a runtime error from ack handler.")

    async def pending_handler(self) -> PendingResponse:
        raise RuntimeError("Got a runtime error from pending handler.")

    async def partitions_handler(self) -> PartitionsResponse:
        raise RuntimeError("Got a runtime error from partition handler.")


class SyncSourceError(Sourcer):
    def read_handler(self, datum: ReadRequest) -> Iterable[Message]:
        raise RuntimeError("Got a runtime error from read handler.")

    def ack_handler(self, ack_request: AckRequest):
        raise RuntimeError("Got a runtime error from ack handler.")

    def pending_handler(self) -> PendingResponse:
        raise RuntimeError("Got a runtime error from pending handler.")

    def partitions_handler(self) -> PartitionsResponse:
        raise RuntimeError("Got a runtime error from partition handler.")
