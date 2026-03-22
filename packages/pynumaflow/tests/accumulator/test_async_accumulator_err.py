import logging
from collections.abc import AsyncIterable

import grpc
import pytest

from pynumaflow import setup_logging
from pynumaflow.accumulator import (
    Message,
    Datum,
    AccumulatorAsyncServer,
    Accumulator,
)
from pynumaflow.proto.accumulator import accumulator_pb2, accumulator_pb2_grpc
from pynumaflow.shared.asynciter import NonBlockingIterator
from tests.conftest import create_async_loop, start_async_server, teardown_async_server
from tests.testing_utils import (
    mock_message,
    get_time_args,
)

pytestmark = pytest.mark.integration

LOGGER = setup_logging(__name__)

SOCK_PATH = "unix:///tmp/accumulator_err.sock"


def request_generator(count, request):
    for i in range(count):
        yield request


def start_request() -> accumulator_pb2.AccumulatorRequest:
    event_time_timestamp, watermark_timestamp = get_time_args()
    window = accumulator_pb2.KeyedWindow(
        start=event_time_timestamp,
        end=watermark_timestamp,
        slot="slot-0",
        keys=["test_key"],
    )
    payload = accumulator_pb2.Payload(
        keys=["test_key"],
        value=mock_message(),
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
        id="test_id",
        headers={"test_header_key": "test_header_value", "source": "test_source"},
    )
    operation = accumulator_pb2.AccumulatorRequest.WindowOperation(
        event=accumulator_pb2.AccumulatorRequest.WindowOperation.Event.OPEN,
        keyedWindow=window,
    )
    request = accumulator_pb2.AccumulatorRequest(
        payload=payload,
        operation=operation,
    )
    return request


class ExampleErrorClass(Accumulator):
    def __init__(self, counter):
        self.counter = counter

    async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
        async for datum in datums:
            self.counter += 1
            if self.counter == 2:
                # Simulate an error on the second datum
                raise RuntimeError("Simulated error in accumulator handler")
            msg = f"counter:{self.counter}"
            await output.put(Message(str.encode(msg), keys=datum.keys, tags=[]))


async def error_accumulator_handler_func(datums: AsyncIterable[Datum], output: NonBlockingIterator):
    counter = 0
    async for datum in datums:
        counter += 1
        if counter == 2:
            # Simulate an error on the second datum
            raise RuntimeError("Simulated error in accumulator function")
        msg = f"counter:{counter}"
        await output.put(Message(str.encode(msg), keys=datum.keys, tags=[]))


def NewAsyncAccumulatorError():
    server_instance = AccumulatorAsyncServer(ExampleErrorClass, init_args=(0,))
    udfs = server_instance.servicer
    return udfs


async def _start_server(udfs):
    server = grpc.aio.server()
    accumulator_pb2_grpc.add_AccumulatorServicer_to_server(udfs, server)
    server.add_insecure_port(SOCK_PATH)
    logging.info("Starting server on %s", SOCK_PATH)
    await server.start()
    return server, SOCK_PATH


@pytest.fixture(scope="module")
def async_accumulator_err_server():
    """Module-scoped fixture: starts an async gRPC accumulator error server."""
    loop = create_async_loop()
    udfs = NewAsyncAccumulatorError()
    server = start_async_server(loop, _start_server(udfs))
    yield loop
    teardown_async_server(loop, server)


@pytest.fixture()
def accumulator_err_stub(async_accumulator_err_server):
    """Returns an AccumulatorStub connected to the running async error server."""
    return accumulator_pb2_grpc.AccumulatorStub(grpc.insecure_channel(SOCK_PATH))


def test_accumulate_partial_success(accumulator_err_stub) -> None:
    """Test that the first datum is processed before error occurs"""
    request = start_request()

    try:
        generator_response = accumulator_err_stub.AccumulateFn(
            request_iterator=request_generator(count=5, request=request)
        )

        # Try to consume the generator
        counter = 0
        for response in generator_response:
            assert isinstance(response, accumulator_pb2.AccumulatorResponse)
            assert response.payload.value.startswith(b"counter:")
            counter += 1

        assert counter == 1, "Expected only one successful response before error"
    except BaseException as err:
        assert "Simulated error in accumulator handler" in str(err)
        return
    pytest.fail("Expected an exception.")
