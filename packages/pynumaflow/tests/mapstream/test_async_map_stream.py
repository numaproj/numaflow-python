import asyncio
import logging
import threading
from collections import Counter
from collections.abc import AsyncIterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow.mapstreamer import (
    Message,
    Datum,
    MapStreamAsyncServer,
)
from pynumaflow.proto.mapper import map_pb2_grpc
from tests.mapstream.utils import request_generator
import pytest

LOGGER = setup_logging(__name__)

# if set to true, map handler will raise a `ValueError` exception.
raise_error_from_map = False

SOCK_PATH = "unix:///tmp/async_map_stream.sock"


async def async_map_stream_handler(keys: list[str], datum: Datum) -> AsyncIterable[Message]:
    val = datum.value
    msg = "payload:{} event_time:{} watermark:{}".format(
        val.decode("utf-8"),
        datum.event_time,
        datum.watermark,
    )
    for i in range(10):
        yield Message(str.encode(msg), keys=keys)


def _startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def _start_server(udfs):
    server = grpc.aio.server()
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    server.add_insecure_port(SOCK_PATH)
    logging.info("Starting server on %s", SOCK_PATH)
    await server.start()
    return server


@pytest.fixture(scope="module")
def async_map_stream_server():
    """Module-scoped fixture: starts an async gRPC map stream server in a background thread."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=_startup_callable, args=(loop,), daemon=True)
    thread.start()

    server_obj = MapStreamAsyncServer(map_stream_instance=async_map_stream_handler)
    udfs = server_obj.servicer
    future = asyncio.run_coroutine_threadsafe(_start_server(udfs), loop=loop)
    future.result(timeout=10)

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
def map_stream_stub(async_map_stream_server):
    """Returns a MapStub connected to the running async map stream server."""
    return map_pb2_grpc.MapStub(grpc.insecure_channel(SOCK_PATH))


def test_map_stream(map_stream_stub):
    # Send >1 requests
    req_count = 3
    try:
        generator_response = map_stream_stub.MapFn(
            request_iterator=request_generator(count=req_count, session=1)
        )
    except grpc.RpcError as e:
        logging.error(e)
        pytest.fail(f"RPC failed: {e}")

    # First message must be the handshake
    handshake = next(generator_response)
    assert handshake.handshake.sot

    # Expected: 10 results per request + 1 EOT per request
    expected_result_msgs = req_count * 10
    expected_eots = req_count

    # Prepare expected payload
    expected_payload = bytes(
        "payload:test_mock_message " "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
        encoding="utf-8",
    )

    id_counter = Counter()
    result_msg_count = 0
    eot_count = 0

    for msg in generator_response:
        # Count EOTs wherever they show up
        if hasattr(msg, "status") and msg.status.eot:
            eot_count += 1
            continue

        # Otherwise, it's a data/result message; validate payload and tally by id
        assert msg.results, "Expected results in MapResponse."
        assert msg.results[0].value == expected_payload
        id_counter[msg.id] += 1
        result_msg_count += 1

    # Validate totals
    assert (
        result_msg_count == expected_result_msgs
    ), f"Expected {expected_result_msgs} result messages, got {result_msg_count}"
    assert eot_count == expected_eots, f"Expected {expected_eots} EOT messages, got {eot_count}"

    # Validate 10 messages per request id: test-id-0..test-id-(req_count-1)
    for i in range(req_count):
        assert (
            id_counter[f"test-id-{i}"] == 10
        ), f"Expected 10 results for test-id-{i}, got {id_counter[f'test-id-{i}']}"


def test_is_ready(async_map_stream_server):
    with grpc.insecure_channel(SOCK_PATH) as channel:
        stub = map_pb2_grpc.MapStub(channel)

        request = _empty_pb2.Empty()
        response = None
        try:
            response = stub.IsReady(request=request)
        except grpc.RpcError as e:
            logging.error(e)

        assert response.ready


@pytest.mark.parametrize(
    "max_threads_arg,expected",
    [
        (32, 16),  # max cap at 16
        (5, 5),  # use argument provided
        (None, 4),  # defaults to 4
    ],
)
def test_max_threads(max_threads_arg, expected):
    kwargs = {"map_stream_instance": async_map_stream_handler}
    if max_threads_arg is not None:
        kwargs["max_threads"] = max_threads_arg
    server = MapStreamAsyncServer(**kwargs)
    assert server.max_threads == expected
