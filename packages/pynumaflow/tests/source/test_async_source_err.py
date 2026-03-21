import asyncio
import logging
import threading

import grpc
import pytest

from pynumaflow import setup_logging
from pynumaflow.proto.sourcer import source_pb2_grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.sourcer import SourceAsyncServer
from tests.source.test_async_source import request_generator
from tests.source.utils import (
    read_req_source_fn,
    ack_req_source_fn,
    AsyncSourceError,
    nack_req_source_fn,
)

pytestmark = pytest.mark.integration

LOGGER = setup_logging(__name__)

server_port = "unix:///tmp/async_err_source.sock"


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def start_server():
    server = grpc.aio.server()
    class_instance = AsyncSourceError()
    server_instance = SourceAsyncServer(sourcer_instance=class_instance)
    udfs = server_instance.servicer
    source_pb2_grpc.add_SourceServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/async_err_source.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    await server.wait_for_termination()


@pytest.fixture(scope="module")
def async_source_err_server():
    """Module-scoped fixture: starts an async gRPC source error server in a background thread."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
    thread.start()

    asyncio.run_coroutine_threadsafe(start_server(), loop=loop)

    while True:
        try:
            with grpc.insecure_channel(server_port) as channel:
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


def test_read_error(async_source_err_server) -> None:
    grpc_exception = None
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)
        request = read_req_source_fn()
        generator_response = None
        try:
            generator_response = stub.ReadFn(request_iterator=request_generator(1, request, "read"))
            for _ in generator_response:
                pass
        except grpc.RpcError as e:
            grpc_exception = e
            assert grpc.StatusCode.INTERNAL == e.code()
            print(e.details())

    assert grpc_exception is not None


def test_read_handshake_error(async_source_err_server) -> None:
    grpc_exception = None
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)
        request = read_req_source_fn()
        generator_response = None
        try:
            generator_response = stub.ReadFn(
                request_iterator=request_generator(1, request, "read", False)
            )
            for _ in generator_response:
                pass
        except BaseException as e:
            assert "ReadFn: expected handshake message" in str(e)
            return
        except grpc.RpcError as e:
            grpc_exception = e
            assert grpc.StatusCode.UNKNOWN == e.code()
            print(e.details())

    assert grpc_exception is not None
    pytest.fail("Expected an exception.")


def test_ack_error(async_source_err_server) -> None:
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)
        request = ack_req_source_fn()
        try:
            resp = stub.AckFn(request_iterator=request_generator(1, request, "ack"))
            for _ in resp:
                pass
        except BaseException as e:
            assert "Got a runtime error from ack handler." in str(e)
            return
        except grpc.RpcError as e:
            assert grpc.StatusCode.UNKNOWN == e.code()
            print(e.details())
    pytest.fail("Expected an exception.")


def test_nack_error(async_source_err_server):
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)
        request = nack_req_source_fn()
        with pytest.raises(grpc.RpcError, match="Got a runtime error from nack handler.") as exc:
            stub.NackFn(request=request)

        assert grpc.StatusCode.INTERNAL == exc.value.code()


def test_ack_no_handshake_error(async_source_err_server) -> None:
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)
        request = ack_req_source_fn()
        try:
            resp = stub.AckFn(request_iterator=request_generator(1, request, "ack", False))
            for _ in resp:
                pass
        except BaseException as e:
            assert "AckFn: expected handshake message" in str(e)
            return
        except grpc.RpcError as e:
            assert grpc.StatusCode.UNKNOWN == e.code()
            print(e.details())
    pytest.fail("Expected an exception.")


def test_pending_error(async_source_err_server) -> None:
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)
        request = _empty_pb2.Empty()
        try:
            stub.PendingFn(request=request)
        except Exception as e:
            assert "Got a runtime error from pending handler." in str(e)
            return
    pytest.fail("Expected an exception.")


def test_partition_error(async_source_err_server) -> None:
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)
        request = _empty_pb2.Empty()
        try:
            stub.PartitionsFn(request=request)
        except Exception as e:
            assert "Got a runtime error from partition handler." in str(e)
            return
    pytest.fail("Expected an exception.")


def test_invalid_server_type() -> None:
    with pytest.raises(TypeError):
        SourceAsyncServer()
