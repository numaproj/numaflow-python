import asyncio
import logging
import threading
from collections.abc import AsyncIterable

import grpc
import pytest
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow.accumulator import (
    Message,
    Datum,
    AccumulatorAsyncServer,
    Accumulator,
)
from pynumaflow.proto.accumulator import accumulator_pb2, accumulator_pb2_grpc
from pynumaflow.shared.asynciter import NonBlockingIterator
from tests.testing_utils import (
    mock_message,
    mock_interval_window_start,
    mock_interval_window_end,
    get_time_args,
)

LOGGER = setup_logging(__name__)

SOCK_PATH = "unix:///tmp/accumulator.sock"


def request_generator(count, request, resetkey: bool = False, send_close: bool = False):
    for i in range(count):
        if resetkey:
            # Update keys on both payload and keyedWindow to match real platform behavior
            del request.payload.keys[:]
            request.payload.keys.extend([f"key-{i}"])
            del request.operation.keyedWindow.keys[:]
            request.operation.keyedWindow.keys.extend([f"key-{i}"])

        # Set operation based on index - first is OPEN, rest are APPEND
        if i == 0:
            request.operation.event = accumulator_pb2.AccumulatorRequest.WindowOperation.Event.OPEN
        else:
            request.operation.event = (
                accumulator_pb2.AccumulatorRequest.WindowOperation.Event.APPEND
            )
        yield request

    if send_close:
        # Send a close operation after all requests
        request.operation.event = accumulator_pb2.AccumulatorRequest.WindowOperation.Event.CLOSE
        yield request


def request_generator_append_only(count, request, resetkey: bool = False):
    for i in range(count):
        if resetkey:
            # Update keys on both payload and keyedWindow to match real platform behavior
            del request.payload.keys[:]
            request.payload.keys.extend([f"key-{i}"])
            del request.operation.keyedWindow.keys[:]
            request.operation.keyedWindow.keys.extend([f"key-{i}"])

        # Set operation to APPEND for all requests
        request.operation.event = accumulator_pb2.AccumulatorRequest.WindowOperation.Event.APPEND
        yield request


def request_generator_mixed(count, request, resetkey: bool = False):
    for i in range(count):
        if resetkey:
            # Update keys on both payload and keyedWindow to match real platform behavior
            del request.payload.keys[:]
            request.payload.keys.extend([f"key-{i}"])
            del request.operation.keyedWindow.keys[:]
            request.operation.keyedWindow.keys.extend([f"key-{i}"])

        if i % 2 == 0:
            # Set operation to APPEND for even requests
            request.operation.event = (
                accumulator_pb2.AccumulatorRequest.WindowOperation.Event.APPEND
            )
        else:
            # Set operation to CLOSE for odd requests
            request.operation.event = accumulator_pb2.AccumulatorRequest.WindowOperation.Event.CLOSE
        yield request


def start_request() -> accumulator_pb2.AccumulatorRequest:
    event_time_timestamp, watermark_timestamp = get_time_args()
    window = accumulator_pb2.KeyedWindow(
        start=mock_interval_window_start(),
        end=mock_interval_window_end(),
        slot="slot-0",
        keys=["test_key"],
    )
    payload = accumulator_pb2.Payload(
        keys=["test_key"],
        value=mock_message(),
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
        id="test_id",
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


def start_request_without_open() -> accumulator_pb2.AccumulatorRequest:
    event_time_timestamp, watermark_timestamp = get_time_args()
    window = accumulator_pb2.KeyedWindow(
        start=mock_interval_window_start(),
        end=mock_interval_window_end(),
        slot="slot-0",
        keys=["test_key"],
    )
    payload = accumulator_pb2.Payload(
        keys=["test_key"],
        value=mock_message(),
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
        id="test_id",
    )
    operation = accumulator_pb2.AccumulatorRequest.WindowOperation(
        event=accumulator_pb2.AccumulatorRequest.WindowOperation.Event.APPEND,
        keyedWindow=window,
    )
    request = accumulator_pb2.AccumulatorRequest(
        payload=payload,
        operation=operation,
    )
    return request


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


class ExampleClass(Accumulator):
    def __init__(self, counter):
        self.counter = counter

    async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
        async for datum in datums:
            self.counter += 1
            msg = f"counter:{self.counter}"
            await output.put(Message(str.encode(msg), keys=datum.keys, tags=[]))


async def accumulator_handler_func(datums: AsyncIterable[Datum], output: NonBlockingIterator):
    counter = 0
    async for datum in datums:
        counter += 1
        msg = f"counter:{counter}"
        await output.put(Message(str.encode(msg), keys=datum.keys, tags=[]))


def NewAsyncAccumulator():
    server_instance = AccumulatorAsyncServer(ExampleClass, init_args=(0,))
    udfs = server_instance.servicer
    return udfs


async def _start_server(udfs):
    server = grpc.aio.server()
    accumulator_pb2_grpc.add_AccumulatorServicer_to_server(udfs, server)
    server.add_insecure_port(SOCK_PATH)
    logging.info("Starting server on %s", SOCK_PATH)
    await server.start()
    return server


@pytest.fixture(scope="module")
def async_accumulator_server():
    """Module-scoped fixture: starts an async gRPC accumulator server in a background thread."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
    thread.start()

    udfs = NewAsyncAccumulator()
    future = asyncio.run_coroutine_threadsafe(_start_server(udfs), loop=loop)
    future.result(timeout=10)

    # Wait for the server to be ready
    while True:
        try:
            with grpc.insecure_channel(SOCK_PATH) as channel:
                f = grpc.channel_ready_future(channel)
                f.result(timeout=10)
                if f.done():
                    break
        except grpc.FutureTimeoutError as e:
            LOGGER.error("error trying to connect to grpc server")
            LOGGER.error(e)

    yield loop

    loop.stop()
    LOGGER.info("stopped the event loop")


@pytest.fixture()
def accumulator_stub(async_accumulator_server):
    """Returns an AccumulatorStub connected to the running async server."""
    return accumulator_pb2_grpc.AccumulatorStub(grpc.insecure_channel(SOCK_PATH))


def test_accumulate(accumulator_stub) -> None:
    request = start_request()
    generator_response = None
    try:
        generator_response = accumulator_stub.AccumulateFn(
            request_iterator=request_generator(count=5, request=request)
        )
    except grpc.RpcError as e:
        logging.error(e)

    # capture the output from the AccumulateFn generator and assert.
    count = 0
    eof_count = 0
    for r in generator_response:
        if hasattr(r, "payload") and r.payload.value:
            count += 1
            # Each datum should increment the counter
            expected_msg = f"counter:{count}"
            assert bytes(expected_msg, encoding="utf-8") == r.payload.value
            assert r.EOF is False
            # Check that keys are preserved
            assert list(r.payload.keys) == ["test_key"]
        else:
            assert r.EOF is True
            eof_count += 1

    # We should have received 5 messages (one for each datum)
    assert 5 == count
    assert 1 == eof_count


def test_accumulate_with_multiple_keys(accumulator_stub) -> None:
    request = start_request()
    generator_response = None
    try:
        generator_response = accumulator_stub.AccumulateFn(
            request_iterator=request_generator(count=10, request=request, resetkey=True),
        )
    except grpc.RpcError as e:
        LOGGER.error(e)

    count = 0
    eof_count = 0
    key_counts = {}

    # capture the output from the AccumulateFn generator and assert.
    for r in generator_response:
        # Check for responses with values
        if r.payload.value:
            count += 1
            # Track count per key
            key = r.payload.keys[0] if r.payload.keys else "no_key"
            key_counts[key] = key_counts.get(key, 0) + 1

            # Each key should have its own counter starting from 1
            expected_msg = f"counter:{key_counts[key]}"
            assert bytes(expected_msg, encoding="utf-8") == r.payload.value
            assert r.EOF is False
        else:
            eof_count += 1
            assert r.EOF is True

    # We should have 10 messages (one for each key)
    assert 10 == count
    assert 10 == eof_count  # Each key/task sends its own EOF
    # Each key should appear once
    assert len(key_counts) == 10


def test_accumulate_with_close(accumulator_stub) -> None:
    request = start_request()
    generator_response = None
    try:
        generator_response = accumulator_stub.AccumulateFn(
            request_iterator=request_generator(count=5, request=request, send_close=True)
        )
    except grpc.RpcError as e:
        logging.error(e)

    # capture the output from the AccumulateFn generator and assert.
    count = 0
    eof_count = 0
    for r in generator_response:
        if hasattr(r, "payload") and r.payload.value:
            count += 1
            # Each datum should increment the counter
            expected_msg = f"counter:{count}"
            assert bytes(expected_msg, encoding="utf-8") == r.payload.value
            assert r.EOF is False
            # Check that keys are preserved
            assert list(r.payload.keys) == ["test_key"]
        else:
            assert r.EOF is True
            eof_count += 1

    # We should have received 5 messages (one for each datum)
    assert 5 == count
    assert 1 == eof_count


def test_accumulate_append_without_open(accumulator_stub) -> None:
    request = start_request_without_open()
    generator_response = None
    try:
        generator_response = accumulator_stub.AccumulateFn(
            request_iterator=request_generator_append_only(count=5, request=request)
        )
    except grpc.RpcError as e:
        logging.error(e)

    # capture the output from the AccumulateFn generator and assert.
    count = 0
    eof_count = 0
    for r in generator_response:
        if hasattr(r, "payload") and r.payload.value:
            count += 1
            # Each datum should increment the counter
            expected_msg = f"counter:{count}"
            assert bytes(expected_msg, encoding="utf-8") == r.payload.value
            assert r.EOF is False
            # Check that keys are preserved
            assert list(r.payload.keys) == ["test_key"]
        else:
            assert r.EOF is True
            eof_count += 1

    # We should have received 5 messages (one for each datum)
    assert 5 == count
    assert 1 == eof_count


def test_accumulate_append_mixed(accumulator_stub) -> None:
    request = start_request()
    generator_response = None
    try:
        generator_response = accumulator_stub.AccumulateFn(
            request_iterator=request_generator_mixed(count=5, request=request)
        )
    except grpc.RpcError as e:
        logging.error(e)

    # capture the output from the AccumulateFn generator and assert.
    count = 0
    eof_count = 0
    for r in generator_response:
        if hasattr(r, "payload") and r.payload.value:
            count += 1
            # Each datum should increment the counter
            expected_msg = "counter:1"
            assert bytes(expected_msg, encoding="utf-8") == r.payload.value
            assert r.EOF is False
            # Check that keys are preserved
            assert list(r.payload.keys) == ["test_key"]
        else:
            assert r.EOF is True
            eof_count += 1

    # We should have received 5 messages (one for each datum)
    assert 3 == count
    assert 3 == eof_count


def test_is_ready(async_accumulator_server) -> None:
    with grpc.insecure_channel(SOCK_PATH) as channel:
        stub = accumulator_pb2_grpc.AccumulatorStub(channel)

        request = _empty_pb2.Empty()
        response = None
        try:
            response = stub.IsReady(request=request)
        except grpc.RpcError as e:
            logging.error(e)

        assert response.ready


def test_error_init():
    # Check that accumulator_instance is required
    with pytest.raises(TypeError):
        AccumulatorAsyncServer()
    # Check that the init_args and init_kwargs are passed
    # only with an Accumulator class
    with pytest.raises(TypeError):
        AccumulatorAsyncServer(accumulator_handler_func, init_args=(0, 1))
    # Check that an instance is not passed instead of the class
    # signature
    with pytest.raises(TypeError):
        AccumulatorAsyncServer(ExampleClass(0))

    # Check that an invalid class is passed
    class ExampleBadClass:
        pass

    with pytest.raises(TypeError):
        AccumulatorAsyncServer(accumulator_instance=ExampleBadClass)


def test_max_threads():
    # max cap at 16
    server = AccumulatorAsyncServer(accumulator_instance=ExampleClass, max_threads=32)
    assert server.max_threads == 16

    # use argument provided
    server = AccumulatorAsyncServer(accumulator_instance=ExampleClass, max_threads=5)
    assert server.max_threads == 5

    # defaults to 4
    server = AccumulatorAsyncServer(accumulator_instance=ExampleClass)
    assert server.max_threads == 4

    # zero threads
    server = AccumulatorAsyncServer(ExampleClass, max_threads=0)
    assert server.max_threads == 0

    # negative threads
    server = AccumulatorAsyncServer(ExampleClass, max_threads=-5)
    assert server.max_threads == -5


def test_server_info_file_path_handling():
    """Test AccumulatorAsyncServer with custom server info file path."""

    server = AccumulatorAsyncServer(
        ExampleClass, init_args=(0,), server_info_file="/custom/path/server_info.json"
    )

    assert server.server_info_file == "/custom/path/server_info.json"


def test_init_kwargs_none_handling():
    """Test init_kwargs None handling in AccumulatorAsyncServer."""

    server = AccumulatorAsyncServer(
        ExampleClass, init_args=(0,), init_kwargs=None  # This should be converted to {}
    )

    # Should not raise any errors and should work correctly
    assert server.accumulator_handler is not None


def test_server_start_method_logging():
    """Test server start method includes proper logging."""
    from unittest.mock import patch

    server = AccumulatorAsyncServer(ExampleClass)

    # Mock aiorun.run to prevent actual server startup
    with (
        patch("pynumaflow.accumulator.async_server.aiorun") as mock_aiorun,
        patch("pynumaflow.accumulator.async_server._LOGGER") as mock_logger,
    ):
        server.start()

        # Verify logging was called
        mock_logger.info.assert_called_once_with("Starting Async Accumulator Server")

        # Verify aiorun.run was called with correct parameters
        mock_aiorun.run.assert_called_once()
        assert mock_aiorun.run.call_args[1]["use_uvloop"]
