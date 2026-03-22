import logging
from collections.abc import Iterator

import grpc
import pytest
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import MAX_MESSAGE_SIZE
from pynumaflow.mapper import (
    Datum,
    Messages,
    Message,
)
from pynumaflow.mapper.async_server import MapAsyncServer
from pynumaflow.proto.common import metadata_pb2
from pynumaflow.proto.mapper import map_pb2, map_pb2_grpc
from tests.conftest import create_async_loop, start_async_server, teardown_async_server
from tests.map.utils import get_test_datums

pytestmark = pytest.mark.integration

LOGGER = setup_logging(__name__)

# if set to true, map handler will raise a `ValueError` exception.
raise_error_from_map = False

SOCK_PATH = "unix:///tmp/async_map.sock"


def request_generator(req):
    yield from req


async def async_map_handler(keys: list[str], datum: Datum) -> Messages:
    if raise_error_from_map:
        raise ValueError("Exception thrown from map")
    val = datum.value
    msg = "payload:{} event_time:{} watermark:{}".format(
        val.decode("utf-8"),
        datum.event_time,
        datum.watermark,
    )
    messages = Messages()
    if datum.system_metadata.value("numaflow_version_info", "version") != b"1.0.0":
        raise ValueError("System metadata version mismatch")
    messages.append(Message(str.encode(msg), keys=keys, user_metadata=datum.user_metadata))
    return messages


async def _start_server(udfs):
    _server_options = [
        ("grpc.max_send_message_length", MAX_MESSAGE_SIZE),
        ("grpc.max_receive_message_length", MAX_MESSAGE_SIZE),
    ]
    server = grpc.aio.server(options=_server_options)
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    server.add_insecure_port(SOCK_PATH)
    logging.info("Starting server on %s", SOCK_PATH)
    await server.start()
    return server, SOCK_PATH


@pytest.fixture(scope="module")
def async_map_server():
    """Module-scoped fixture: starts an async gRPC map server in a background thread."""
    loop = create_async_loop()

    server_obj = MapAsyncServer(mapper_instance=async_map_handler)
    udfs = server_obj.servicer
    server = start_async_server(loop, _start_server(udfs))

    yield loop

    teardown_async_server(loop, server)


@pytest.fixture()
def map_stub(async_map_server):
    """Returns a MapStub connected to the running async server."""
    return map_pb2_grpc.MapStub(grpc.insecure_channel(SOCK_PATH))


def test_run_server(async_map_server):
    with grpc.insecure_channel(SOCK_PATH) as channel:
        stub = map_pb2_grpc.MapStub(channel)
        request = get_test_datums()
        generator_response = stub.MapFn(request_iterator=request_generator(request))

        responses = list(generator_response)

        # 1 handshake + 3 data responses
        assert len(responses) == 4
        assert responses[0].handshake.sot

        idx = 1
        while idx < len(responses):
            assert responses[idx].id == "test-id-" + str(idx)
            assert responses[idx].results[0].value == bytes(
                "payload:test_mock_message "
                "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                encoding="utf-8",
            )
            assert len(responses[idx].results) == 1
            idx += 1
        LOGGER.info("Successfully validated the server")


def test_map(map_stub):
    request = get_test_datums()
    generator_response: Iterator[map_pb2.MapResponse] = map_stub.MapFn(
        request_iterator=request_generator(request)
    )

    responses: list[map_pb2.MapResponse] = list(generator_response)

    # 1 handshake + 3 data responses
    assert len(responses) == 4
    assert responses[0].handshake.sot

    for idx, resp in enumerate(responses[1:], 1):
        assert resp.id == "test-id-" + str(idx)
        assert resp.results[0].value == bytes(
            "payload:test_mock_message "
            "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
            encoding="utf-8",
        )
        assert len(resp.results) == 1
        assert resp.results[0].metadata.user_metadata["custom_info"] == metadata_pb2.KeyValueGroup(
            key_value={"version": f"{idx}.0.0".encode()}
        )
        # System metadata will be empty for user responses
        assert resp.results[0].metadata.sys_metadata == {}


def test_map_grpc_error_no_handshake(map_stub):
    request = get_test_datums(handshake=False)
    grpc_exception = None

    responses = []
    try:
        generator_response = map_stub.MapFn(request_iterator=request_generator(request))
        for r in generator_response:
            responses.append(r)
    except grpc.RpcError as e:
        logging.error(e)
        grpc_exception = e
        assert "MapFn: expected handshake as the first message" in str(e)

    assert len(responses) == 0
    assert grpc_exception is not None


def test_map_grpc_error(map_stub):
    request = get_test_datums()
    grpc_exception = None

    responses = []
    try:
        global raise_error_from_map
        raise_error_from_map = True
        generator_response = map_stub.MapFn(request_iterator=request_generator(request))
        for r in generator_response:
            responses.append(r)
    except grpc.RpcError as e:
        logging.error(e)
        grpc_exception = e
        assert e.code() == grpc.StatusCode.INTERNAL
        assert "Exception thrown from map" in str(e)
    finally:
        raise_error_from_map = False
    # 1 handshake
    assert len(responses) == 1
    assert grpc_exception is not None


def test_is_ready(async_map_server):
    with grpc.insecure_channel(SOCK_PATH) as channel:
        stub = map_pb2_grpc.MapStub(channel)
        response = stub.IsReady(request=_empty_pb2.Empty())
        assert response.ready


def test_invalid_input():
    with pytest.raises(TypeError):
        MapAsyncServer()


@pytest.mark.parametrize(
    "max_threads_arg,expected",
    [
        (32, 16),  # max cap at 16
        (5, 5),  # use argument provided
        (None, 4),  # defaults to 4
    ],
)
def test_max_threads(max_threads_arg, expected):
    kwargs = {"mapper_instance": async_map_handler}
    if max_threads_arg is not None:
        kwargs["max_threads"] = max_threads_arg
    server = MapAsyncServer(**kwargs)
    assert server.max_threads == expected
