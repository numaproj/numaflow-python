import asyncio
from collections.abc import Iterator
import logging
import threading

import grpc
import pytest
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._metadata import _user_and_system_metadata_from_proto
from pynumaflow.proto.sourcer import source_pb2_grpc, source_pb2
from pynumaflow.sourcer import (
    SourceAsyncServer,
)
from tests.source.utils import (
    read_req_source_fn,
    ack_req_source_fn,
    mock_partitions,
    AsyncSource,
    mock_offset,
    nack_req_source_fn,
)

pytestmark = pytest.mark.integration

LOGGER = setup_logging(__name__)

server_port = "unix:///tmp/async_source.sock"


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def NewAsyncSourcer():
    class_instance = AsyncSource()
    server = SourceAsyncServer(sourcer_instance=class_instance)
    udfs = server.servicer
    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    source_pb2_grpc.add_SourceServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/async_source.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    await server.wait_for_termination()


def request_generator(count, request, req_type, send_handshake: bool = True):
    for i in range(count):
        if req_type == "read":
            if send_handshake:
                yield source_pb2.ReadRequest(handshake=source_pb2.Handshake(sot=True))
            yield source_pb2.ReadRequest(request=request)
        elif req_type == "ack":
            if send_handshake:
                yield source_pb2.AckRequest(handshake=source_pb2.Handshake(sot=True))
            yield source_pb2.AckRequest(request=request)


@pytest.fixture(scope="module")
def async_source_server():
    """Module-scoped fixture: starts an async gRPC source server in a background thread."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
    thread.start()

    udfs = NewAsyncSourcer()
    asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)

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


def test_read_source(async_source_server) -> None:
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)

        request = read_req_source_fn()
        try:
            generator_response: Iterator[source_pb2.ReadResponse] = stub.ReadFn(
                request_iterator=request_generator(1, request, "read")
            )
        except grpc.RpcError as e:
            logging.error(e)
            raise

        counter = 0
        first = True
        last = False
        # capture the output from the ReadFn generator and assert.
        for r in generator_response:
            counter += 1
            if first:
                assert r.handshake.sot is True
                first = False
                continue

            if r.status.eot:
                last = True
                continue

            assert bytes("payload:test_mock_message", encoding="utf-8") == r.result.payload
            assert ["test_key"] == r.result.keys
            assert mock_offset().offset == r.result.offset.offset
            assert mock_offset().partition_id == r.result.offset.partition_id

            print(r.result)
            user_metadata, sys_metadata = _user_and_system_metadata_from_proto(r.result.metadata)
            print(user_metadata)

            assert sorted(user_metadata.groups()) == sorted(["custom_info", "test_info"])
            assert sorted(user_metadata.keys("custom_info")) == sorted(
                ["custom_key", "custom_key2"]
            )
            assert user_metadata.value("custom_info", "test_key") is None
            assert user_metadata.value("custom_info", "custom_key") == b"custom_value"
            assert user_metadata.value("test_info", "test_key") == b"test_value"

        assert not first
        assert last

        # Assert that the generator was called 12
        # (10 data messages + handshake + eot) times in the stream
        assert 12 == counter


def test_is_ready(async_source_server) -> None:
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)

        request = _empty_pb2.Empty()
        response = None
        try:
            response = stub.IsReady(request=request)
        except grpc.RpcError as e:
            logging.error(e)

        assert response.ready


def test_ack(async_source_server) -> None:
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)
        request = ack_req_source_fn()
        try:
            response = stub.AckFn(request_iterator=request_generator(1, request, "ack"))
        except grpc.RpcError as e:
            print(e)

        count = 0
        first = True
        for r in response:
            count += 1
            if first:
                assert r.handshake.sot is True
                first = False
                continue
            assert r.result.success

        assert count == 2
        assert not first


def test_nack(async_source_server) -> None:
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)
        request = nack_req_source_fn()
        response = stub.NackFn(request=request)
        assert response.result.success


def test_pending(async_source_server) -> None:
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)
        request = _empty_pb2.Empty()
        response = None
        try:
            response = stub.PendingFn(request=request)
        except grpc.RpcError as e:
            logging.error(e)

        assert response.result.count == 10


def test_partitions(async_source_server) -> None:
    with grpc.insecure_channel(server_port) as channel:
        stub = source_pb2_grpc.SourceStub(channel)
        request = _empty_pb2.Empty()
        response = None
        try:
            response = stub.PartitionsFn(request=request)
        except grpc.RpcError as e:
            logging.error(e)

        assert response.result.partitions == mock_partitions()


@pytest.mark.parametrize(
    "max_threads_arg,expected",
    [
        (32, 16),  # max cap at 16
        (5, 5),  # use argument provided
        (None, 4),  # defaults to 4
    ],
)
def test_max_threads(max_threads_arg, expected):
    class_instance = AsyncSource()
    kwargs = {"sourcer_instance": class_instance}
    if max_threads_arg is not None:
        kwargs["max_threads"] = max_threads_arg
    server = SourceAsyncServer(**kwargs)
    assert server.max_threads == expected
