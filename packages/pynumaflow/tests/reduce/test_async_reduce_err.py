import asyncio
import logging
import threading
from collections.abc import AsyncIterable

import grpc
import pytest

from pynumaflow import setup_logging
from pynumaflow._constants import WIN_START_TIME, WIN_END_TIME
from pynumaflow.reducer import (
    Messages,
    Message,
    Datum,
    Metadata,
    ReduceAsyncServer,
)
from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from tests.testing_utils import (
    mock_message,
    mock_interval_window_start,
    mock_interval_window_end,
    get_time_args,
)

pytestmark = pytest.mark.integration

LOGGER = setup_logging(__name__)

SOCK_PATH = "unix:///tmp/reduce_err.sock"


def request_generator(count, request, resetkey: bool = False):
    for i in range(count):
        if resetkey:
            request.keys.extend([f"key-{i}"])
        yield request


def start_request(multiple_window: False) -> (Datum, tuple):
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
    if multiple_window:
        operation = reduce_pb2.ReduceRequest.WindowOperation(
            event=reduce_pb2.ReduceRequest.WindowOperation.Event.APPEND,
            windows=[window, window],
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


async def err_handler(keys: list[str], datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    raise RuntimeError("Got a runtime error from reduce handler.")
    return Messages(Message(str.encode(msg), keys=keys))


def NewAsyncReducer():
    server_instance = ReduceAsyncServer(err_handler)
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
def async_reduce_err_server():
    """Module-scoped fixture: starts an async gRPC reduce error server in a background thread."""
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
def reduce_err_stub(async_reduce_err_server):
    """Returns a ReduceStub connected to the running async error server."""
    return reduce_pb2_grpc.ReduceStub(grpc.insecure_channel(SOCK_PATH))


def test_reduce(async_reduce_err_server) -> None:
    with grpc.insecure_channel(SOCK_PATH) as channel:
        stub = reduce_pb2_grpc.ReduceStub(channel)
        request, metadata = start_request(multiple_window=False)
        generator_response = None
        try:
            generator_response = stub.ReduceFn(
                request_iterator=request_generator(count=1, request=request)
            )
            counter = 0
            for _ in generator_response:
                counter += 1
        except BaseException as err:
            assert "Got a runtime error from reduce handler." in str(err)
            return
        pytest.fail("Expected an exception.")


def test_reduce_window_len(reduce_err_stub) -> None:
    request, metadata = start_request(multiple_window=True)
    generator_response = None
    try:
        generator_response = reduce_err_stub.ReduceFn(
            request_iterator=request_generator(count=10, request=request)
        )
        counter = 0
        for _ in generator_response:
            counter += 1
    except BaseException as err:
        assert "reduce create operation error: invalid number of windows" in str(err)
        return
    pytest.fail("Expected an exception.")
