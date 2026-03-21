import asyncio
import logging
import threading
from collections.abc import AsyncIterable

import grpc
import pytest
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow._constants import WIN_START_TIME, WIN_END_TIME
from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from pynumaflow.reducer import (
    Messages,
    Message,
    Datum,
    Metadata,
    ReduceAsyncServer,
    Reducer,
)
from tests.testing_utils import (
    mock_message,
    mock_interval_window_start,
    mock_interval_window_end,
    get_time_args,
)

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

SOCK_PATH = "unix:///tmp/reduce.sock"


def request_generator(count, request, resetkey: bool = False):
    for i in range(count):
        if resetkey:
            request.payload.keys.extend([f"key-{i}"])
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
        event=reduce_pb2.ReduceRequest.WindowOperation.Event.OPEN,
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


class ExampleClass(Reducer):
    def __init__(self, counter):
        self.counter = counter

    async def handler(
        self, keys: list[str], datums: AsyncIterable[Datum], md: Metadata
    ) -> Messages:
        self.counter = 0
        async for _ in datums:
            self.counter += 1
        msg = f"counter:{self.counter}"
        return Messages(Message(str.encode(msg), keys=keys))


async def reduce_handler_func(
    keys: list[str], datums: AsyncIterable[Datum], md: Metadata
) -> Messages:
    counter = 0
    async for _ in datums:
        counter += 1
    msg = f"counter:{counter}"
    return Messages(Message(str.encode(msg), keys=keys))


def NewAsyncReducer():
    server_instance = ReduceAsyncServer(ExampleClass, init_args=(0,))
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
def async_reduce_server():
    """Module-scoped fixture: starts an async gRPC reduce server in a background thread."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
    thread.start()

    udfs = NewAsyncReducer()
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
def reduce_stub(async_reduce_server):
    """Returns a ReduceStub connected to the running async server."""
    return reduce_pb2_grpc.ReduceStub(grpc.insecure_channel(SOCK_PATH))


def test_reduce(reduce_stub) -> None:
    request, metadata = start_request()
    generator_response = None
    try:
        generator_response = reduce_stub.ReduceFn(
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
            assert (
                bytes(
                    "counter:10",
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
    # since there is only one key, the output count is 1
    assert 1 == count
    assert 1 == eof_count


def test_reduce_with_multiple_keys(reduce_stub) -> None:
    request, metadata = start_request()
    generator_response = None
    try:
        generator_response = reduce_stub.ReduceFn(
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


def test_is_ready(async_reduce_server) -> None:
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
        ReduceAsyncServer()
    # Check that the init_args and init_kwargs are passed
    # only with a Reducer class
    with pytest.raises(TypeError):
        ReduceAsyncServer(reduce_handler_func, init_args=(0, 1))
    # Check that an instance is not passed instead of the class
    # signature
    with pytest.raises(TypeError):
        ReduceAsyncServer(ExampleClass(0))

    # Check that an invalid class is passed
    class ExampleBadClass:
        pass

    with pytest.raises(TypeError):
        ReduceAsyncServer(reducer_instance=ExampleBadClass)


@pytest.mark.parametrize(
    "max_threads_arg,expected",
    [
        (32, 16),  # max cap at 16
        (5, 5),  # use argument provided
        (None, 4),  # defaults to 4
    ],
)
def test_max_threads(max_threads_arg, expected):
    kwargs = {"reducer_instance": ExampleClass}
    if max_threads_arg is not None:
        kwargs["max_threads"] = max_threads_arg
    server = ReduceAsyncServer(**kwargs)
    assert server.max_threads == expected
