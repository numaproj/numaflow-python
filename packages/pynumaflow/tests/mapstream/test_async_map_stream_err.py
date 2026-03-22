import logging
from collections.abc import AsyncIterable

import grpc
import pytest

from pynumaflow import setup_logging
from pynumaflow.mapstreamer import Message, Datum, MapStreamAsyncServer
from pynumaflow.proto.mapper import map_pb2_grpc
from tests.mapstream.utils import request_generator
from tests.conftest import create_async_loop, start_async_server, teardown_async_server

pytestmark = pytest.mark.integration

LOGGER = setup_logging(__name__)

SOCK_PATH = "unix:///tmp/async_map_stream_err.sock"


# This handler mimics the scenario where map stream UDF throws a runtime error.
async def err_async_map_stream_handler(keys: list[str], datum: Datum) -> AsyncIterable[Message]:
    val = datum.value
    msg = "payload:{} event_time:{} watermark:{}".format(
        val.decode("utf-8"),
        datum.event_time,
        datum.watermark,
    )

    for i in range(5):
        yield Message(str.encode(msg), keys=keys)

    raise RuntimeError("Got a runtime error from map stream handler.")


async def _start_server():
    server = grpc.aio.server()
    server_instance = MapStreamAsyncServer(err_async_map_stream_handler)
    udfs = server_instance.servicer
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    server.add_insecure_port(SOCK_PATH)
    logging.info("Starting server on %s", SOCK_PATH)
    await server.start()
    return server, SOCK_PATH


@pytest.fixture(scope="module")
def async_map_stream_err_server():
    """Module-scoped fixture: starts an async gRPC map stream error server."""
    loop = create_async_loop()
    server = start_async_server(loop, _start_server())
    yield loop
    teardown_async_server(loop, server)


@pytest.fixture()
def map_stream_err_stub(async_map_stream_err_server):
    """Returns a MapStub connected to the running async error server."""
    return map_pb2_grpc.MapStub(grpc.insecure_channel(SOCK_PATH))


def test_map_stream_error(map_stream_err_stub):
    try:
        generator_response = None
        try:
            generator_response = map_stream_err_stub.MapFn(
                request_iterator=request_generator(count=1, session=1)
            )
        except grpc.RpcError as e:
            logging.error(e)

        handshake = next(generator_response)
        # assert that handshake response is received.
        assert handshake.handshake.sot
        data_resp = []
        for r in generator_response:
            data_resp.append(r)
    except Exception as err:
        assert "Got a runtime error from map stream handler." in str(err)
        return
    pytest.fail("Expected an exception.")


def test_map_stream_error_no_handshake(map_stream_err_stub):
    try:
        generator_response = map_stream_err_stub.MapFn(
            request_iterator=request_generator(count=10, handshake=False, session=1)
        )
        counter = 0
        for _ in generator_response:
            counter += 1
    except Exception as err:
        assert "MapStreamFn: expected handshake as the first message" in str(err)
        return
    pytest.fail("Expected an exception.")


def test_invalid_input():
    with pytest.raises(TypeError):
        MapStreamAsyncServer()
