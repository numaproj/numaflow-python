from collections.abc import AsyncIterable, Iterable

from pynumaflow.sourcer import ReadRequest, Message
from pynumaflow.sourcer._dtypes import AckRequest, PendingResponse, Offset
from pynumaflow.sourcer.proto import source_pb2
from tests.testing_utils import mock_event_time


def mock_offset() -> Offset:
    return Offset(b"offset_mock", "10")


async def async_source_read_handler(datum: ReadRequest) -> AsyncIterable[Message]:
    payload = b"payload:test_mock_message"
    keys = ["test_key"]
    offset = mock_offset()
    event_time = mock_event_time()
    for i in range(10):
        yield Message(payload=payload, keys=keys, offset=offset, event_time=event_time)


async def async_source_ack_handler(ack_request: AckRequest):
    return


async def async_source_pending_handler() -> PendingResponse:
    return PendingResponse(count=10)


def sync_source_read_handler(datum: ReadRequest) -> Iterable[Message]:
    payload = b"payload:test_mock_message"
    keys = ["test_key"]
    offset = mock_offset()
    event_time = mock_event_time()
    for i in range(10):
        yield Message(payload=payload, keys=keys, offset=offset, event_time=event_time)


def sync_source_ack_handler(ack_request: AckRequest):
    return


def sync_source_pending_handler() -> PendingResponse:
    return PendingResponse(count=10)


def read_req_source_fn() -> ReadRequest:
    request = source_pb2.ReadRequest.Request(
        num_records=10,
        timeout_in_ms=1000,
    )
    return request


def ack_req_source_fn() -> AckRequest:
    msg = source_pb2.Offset(offset=mock_offset().offset, partition_id=mock_offset().partition_id)
    request = source_pb2.AckRequest.Request(offsets=[msg, msg])
    return request


# This handler mimics the scenario where map stream UDF throws a runtime error.
async def err_async_source_read_handler(datum: ReadRequest) -> AsyncIterable[Message]:
    payload = b"payload:test_mock_message"
    keys = ["test_key"]
    offset = mock_offset()
    event_time = mock_event_time()
    for i in range(10):
        yield Message(payload=payload, keys=keys, offset=offset, event_time=event_time)
    raise RuntimeError("Got a runtime error from read handler.")


async def err_async_source_ack_handler(ack_request: AckRequest):
    raise RuntimeError("Got a runtime error from ack handler.")


async def err_async_source_pending_handler() -> PendingResponse:
    raise RuntimeError("Got a runtime error from pending handler.")


def err_sync_source_read_handler(datum: ReadRequest) -> Iterable[Message]:
    raise RuntimeError("Got a runtime error from read handler.")


def err_sync_source_ack_handler(ack_request: AckRequest):
    raise RuntimeError("Got a runtime error from ack handler.")


def err_sync_source_pending_handler() -> PendingResponse:
    raise RuntimeError("Got a runtime error from pending handler.")
