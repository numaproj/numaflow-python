import logging

import grpc
import pytest

from pynumaflow import setup_logging
from pynumaflow.batchmapper import BatchResponses
from pynumaflow.batchmapper import BatchMapAsyncServer
from pynumaflow.proto.mapper import map_pb2_grpc
from tests.batchmap.utils import request_generator
from tests.conftest import create_async_loop, start_async_server, teardown_async_server

pytestmark = pytest.mark.integration

LOGGER = setup_logging(__name__)

raise_error = False


# This handler mimics the scenario where batch map UDF throws a runtime error.
async def err_handler(datums) -> BatchResponses:
    if raise_error:
        raise RuntimeError("Got a runtime error from batch map handler.")
    batch_responses = BatchResponses()
    return batch_responses


listen_addr = "unix:///tmp/async_batch_map_err.sock"


async def start_server():
    server = grpc.aio.server()
    server_instance = BatchMapAsyncServer(err_handler)
    udfs = server_instance.servicer
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    return server, listen_addr


@pytest.fixture(scope="module")
def async_batch_map_err_server():
    """Module-scoped fixture: starts an async gRPC batch map error server in a background thread."""
    loop = create_async_loop()
    server = start_async_server(loop, start_server())
    yield loop
    teardown_async_server(loop, server)


@pytest.fixture()
def batch_map_err_stub(async_batch_map_err_server):
    """Returns a MapStub connected to the running async batch map error server."""
    return map_pb2_grpc.MapStub(grpc.insecure_channel(listen_addr))


def test_batch_map_error(batch_map_err_stub) -> None:
    global raise_error
    raise_error = True
    try:
        generator_response = batch_map_err_stub.MapFn(
            request_iterator=request_generator(count=10, handshake=True, session=1)
        )
        counter = 0
        for _ in generator_response:
            counter += 1
    except Exception as err:
        assert "Got a runtime error from batch map handler." in str(err)
        return
    pytest.fail("Expected an exception.")


def test_batch_map_error_no_handshake(batch_map_err_stub) -> None:
    global raise_error
    raise_error = True
    try:
        generator_response = batch_map_err_stub.MapFn(
            request_iterator=request_generator(count=10, handshake=False, session=1)
        )
        counter = 0
        for _ in generator_response:
            counter += 1
    except Exception as err:
        assert "BatchMapFn: expected handshake as the first message" in str(err)
        return
    pytest.fail("Expected an exception.")


def test_invalid_input():
    with pytest.raises(TypeError):
        BatchMapAsyncServer()
