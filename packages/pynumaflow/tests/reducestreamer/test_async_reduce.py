import asyncio
import logging
import threading
from collections.abc import AsyncIterable

import grpc
import pytest
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import WIN_START_TIME, WIN_END_TIME
from pynumaflow.reducestreamer import (
    Message,
    Datum,
    ReduceStreamAsyncServer,
    ReduceStreamer,
    Metadata,
)
from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from pynumaflow.shared.asynciter import NonBlockingIterator
from tests.testing_utils import (
    mock_message,
    mock_interval_window_start,
    mock_interval_window_end,
    get_time_args,
)

LOGGER = setup_logging(__name__)

SOCK_PATH = "unix:///tmp/reduce_stream.sock"


def request_generator(count, request, resetkey: bool = False):
    for i in range(count):
        if resetkey:
            request.payload.keys.extend([f"key-{i}"])

        if i % 2:
            request.operation.event = reduce_pb2.ReduceRequest.WindowOperation.Event.OPEN
        else:
            request.operation.event = reduce_pb2.ReduceRequest.WindowOperation.Event.APPEND
        yield request


def start_request() -> (Datum, tuple):
    event_time_timestamp, watermark_timestamp = get_time_args()
    window = reduce_pb2.Window(
        start=mock_interval_window_start(),
        end=mock_interval_window_end(),
        slot="slot-0",
    )
    payload = reduce_pb2.ReduceRequest.Payload(
        value=mock_message(),
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
    )
    operation = reduce_pb2.ReduceRequest.WindowOperation(
        event=reduce_pb2.ReduceRequest.WindowOperation.Event.APPEND,
        windows=[window],
    )
    request = reduce_pb2.ReduceRequest(
        payload=payload,
        operation=operation,
    )
    metadata = (
        (WIN_START_TIME, f"{mock_interval_window_start()}"),
        (WIN_END_TIME, f"{mock_interval_window_end()}"),
    )
    return request, metadata


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


class ExampleClass(ReduceStreamer):
    def __init__(self, counter):
        self.counter = counter

    async def handler(
        self,
        keys: list[str],
        datums: AsyncIterable[Datum],
        output: NonBlockingIterator,
        md: Metadata,
    ):
        # print(md.start)
        async for _ in datums:
            self.counter += 1
            if self.counter > 2:
                msg = f"counter:{self.counter}"
                await output.put(Message(str.encode(msg), keys=keys))
                self.counter = 0
        msg = f"counter:{self.counter}"
        await output.put(Message(str.encode(msg), keys=keys))


async def reduce_handler_func(
    keys: list[str],
    datums: AsyncIterable[Datum],
    output: NonBlockingIterator,
    md: Metadata,
):
    counter = 0
    async for _ in datums:
        counter += 1
        if counter > 2:
            msg = f"counter:{counter}"
            await output.put(Message(str.encode(msg), keys=keys))
            counter = 0
    msg = f"counter:{counter}"
    await output.put(Message(str.encode(msg), keys=keys))


def NewAsyncReduceStreamer():
    server_instance = ReduceStreamAsyncServer(ExampleClass, init_args=(0,))
    udfs = server_instance.servicer

    return udfs


async def _start_server(udfs):
    server = grpc.aio.server()
    reduce_pb2_grpc.add_ReduceServicer_to_server(udfs, server)
    server.add_insecure_port(SOCK_PATH)
    logging.info("Starting server on %s", SOCK_PATH)
    await server.start()
    return server


@pytest.fixture(scope="module")
def async_reduce_stream_server():
    """Module-scoped fixture: starts an async gRPC reduce stream server in a background thread."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
    thread.start()

    udfs = NewAsyncReduceStreamer()
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
def reduce_stream_stub(async_reduce_stream_server):
    """Returns a ReduceStub connected to the running async reduce stream server."""
    return reduce_pb2_grpc.ReduceStub(grpc.insecure_channel(SOCK_PATH))


def test_reduce(reduce_stream_stub) -> None:
    request, metadata = start_request()
    generator_response = None

    try:
        generator_response = reduce_stream_stub.ReduceFn(
            request_iterator=request_generator(count=10, request=request)
        )
    except grpc.RpcError as e:
        logging.error(e)

    # capture the output from the ReduceFn generator and assert.
    count = 0
    eof_count = 0
    for r in generator_response:
        if r.result.value:
            count += 1
            if count <= 3:
                assert (
                    bytes(
                        "counter:3",
                        encoding="utf-8",
                    )
                    == r.result.value
                )
            else:
                assert (
                    bytes(
                        "counter:1",
                        encoding="utf-8",
                    )
                    == r.result.value
                )
            assert r.EOF is False
        else:
            assert r.EOF is True
            eof_count += 1
        assert r.window.start.ToSeconds() == 1662998400
        assert r.window.end.ToSeconds() == 1662998460
    # in our example we should be return 3 messages early with counter:3
    # and last message with counter:1
    assert 4 == count
    assert 1 == eof_count


def test_reduce_with_multiple_keys(reduce_stream_stub) -> None:
    request, metadata = start_request()
    generator_response = None
    try:
        generator_response = reduce_stream_stub.ReduceFn(
            request_iterator=request_generator(count=100, request=request, resetkey=True),
        )
    except grpc.RpcError as e:
        print(e)

    count = 0
    eof_count = 0

    # capture the output from the ReduceFn generator and assert.
    for r in generator_response:
        # Check for responses with
        if r.result.value:
            count += 1
            assert (
                bytes(
                    "counter:1",
                    encoding="utf-8",
                )
                == r.result.value
            )
            assert r.EOF is False
        else:
            eof_count += 1
            assert r.EOF is True
        assert r.window.start.ToSeconds() == 1662998400
        assert r.window.end.ToSeconds() == 1662998460
    assert 100 == count
    assert 1 == eof_count


def test_is_ready(async_reduce_stream_server) -> None:
    with grpc.insecure_channel(SOCK_PATH) as channel:
        stub = reduce_pb2_grpc.ReduceStub(channel)

        request = _empty_pb2.Empty()
        response = None
        try:
            response = stub.IsReady(request=request)
        except grpc.RpcError as e:
            logging.error(e)

        assert response.ready


def test_error_init():
    # Check that reducer_instance in required
    with pytest.raises(TypeError):
        ReduceStreamAsyncServer()
    # Check that the init_args and init_kwargs are passed
    # only with a Reducer class
    with pytest.raises(TypeError):
        ReduceStreamAsyncServer(reduce_handler_func, init_args=(0, 1))
    # Check that an instance is not passed instead of the class
    # signature
    with pytest.raises(TypeError):
        ReduceStreamAsyncServer(ExampleClass(0))

    # Check that an invalid class is passed
    class ExampleBadClass:
        pass

    with pytest.raises(TypeError):
        ReduceStreamAsyncServer(reduce_stream_instance=ExampleBadClass)


def test_max_threads():
    # max cap at 16
    server = ReduceStreamAsyncServer(reduce_stream_instance=ExampleClass, max_threads=32)
    assert server.max_threads == 16

    # use argument provided
    server = ReduceStreamAsyncServer(reduce_stream_instance=ExampleClass, max_threads=5)
    assert server.max_threads == 5

    # defaults to 4
    server = ReduceStreamAsyncServer(reduce_stream_instance=ExampleClass)
    assert server.max_threads == 4


def test_start_shutdown_handler_without_callback():
    """Test that _shutdown_handler logs and works when no shutdown_callback is set."""
    from unittest.mock import patch, MagicMock

    server = ReduceStreamAsyncServer(reduce_stream_instance=ExampleClass)
    assert server.shutdown_callback is None

    def close_coro(coro, **kwargs):
        coro.close()

    with patch("pynumaflow.reducestreamer.async_server.aiorun") as mock_aiorun:
        mock_aiorun.run.side_effect = close_coro
        server.start()

        # Extract the shutdown_callback passed to aiorun.run
        call_kwargs = mock_aiorun.run.call_args[1]
        shutdown_handler = call_kwargs["shutdown_callback"]

        # Invoke the handler — should not raise even without a callback
        mock_loop = MagicMock()
        shutdown_handler(mock_loop)


def test_start_shutdown_handler_with_callback():
    """Test that _shutdown_handler invokes the user-provided shutdown_callback."""
    from unittest.mock import patch, MagicMock

    user_callback = MagicMock()
    server = ReduceStreamAsyncServer(
        reduce_stream_instance=ExampleClass, shutdown_callback=user_callback
    )

    def close_coro(coro, **kwargs):
        coro.close()

    with patch("pynumaflow.reducestreamer.async_server.aiorun") as mock_aiorun:
        mock_aiorun.run.side_effect = close_coro
        server.start()

        shutdown_handler = mock_aiorun.run.call_args[1]["shutdown_callback"]
        mock_loop = MagicMock()
        shutdown_handler(mock_loop)

        user_callback.assert_called_once_with(mock_loop)


def test_start_exits_on_error():
    """Test that start() calls sys.exit(1) when servicer reports an error."""
    from unittest.mock import patch

    server = ReduceStreamAsyncServer(reduce_stream_instance=ExampleClass)

    def fake_aiorun_run(coro, **kwargs):
        # Simulate aiorun completing after a UDF error was recorded
        coro.close()  # prevent "coroutine never awaited" warning
        server._error = RuntimeError("UDF failure")

    with (
        patch("pynumaflow.reducestreamer.async_server.aiorun") as mock_aiorun,
        patch("pynumaflow.reducestreamer.async_server.sys") as mock_sys,
    ):
        mock_aiorun.run.side_effect = fake_aiorun_run
        server.start()

        mock_sys.exit.assert_called_once_with(1)
