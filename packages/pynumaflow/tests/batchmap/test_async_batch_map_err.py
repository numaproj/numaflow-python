import asyncio
import logging
import threading

import grpc
import pytest

from pynumaflow import setup_logging
from pynumaflow.batchmapper import BatchResponses
from pynumaflow.batchmapper import BatchMapAsyncServer
from pynumaflow.proto.mapper import map_pb2_grpc
from tests.batchmap.utils import request_generator

LOGGER = setup_logging(__name__)

raise_error = False


# This handler mimics the scenario where batch map UDF throws a runtime error.
async def err_handler(datums) -> BatchResponses:
    if raise_error:
        raise RuntimeError("Got a runtime error from batch map handler.")
    batch_responses = BatchResponses()
    return batch_responses


listen_addr = "unix:///tmp/async_batch_map_err.sock"


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def start_server():
    server = grpc.aio.server()
    server_instance = BatchMapAsyncServer(err_handler)
    udfs = server_instance.servicer
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    await server.wait_for_termination()


@pytest.fixture(scope="module")
def async_batch_map_err_server():
    """Module-scoped fixture: starts an async gRPC batch map error server in a background thread."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
    thread.start()

    asyncio.run_coroutine_threadsafe(start_server(), loop=loop)

    while True:
        try:
            with grpc.insecure_channel(listen_addr) as channel:
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
