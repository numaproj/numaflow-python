import logging
from collections.abc import AsyncIterable

import grpc
import pytest
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow.batchmapper import (
    Message,
    Datum,
    BatchMapper,
    BatchResponses,
    BatchResponse,
    BatchMapAsyncServer,
    NackOptions,
)
from pynumaflow._constants import NACK
from pynumaflow.proto.mapper import map_pb2_grpc
from tests.batchmap.utils import request_generator, metadata_request_generator
from tests.conftest import create_async_loop, start_async_server, teardown_async_server

pytestmark = pytest.mark.integration

LOGGER = setup_logging(__name__)

listen_addr = "unix:///tmp/batch_map.sock"
nack_listen_addr = "unix:///tmp/batch_map_nack.sock"
metadata_listen_addr = "unix:///tmp/batch_map_metadata.sock"

NACK_TEST_OPTIONS = NackOptions(delay=1000, max_deliveries=3, reason="retry")


class ExampleClass(BatchMapper):
    async def handler(
        self,
        datums: AsyncIterable[Datum],
    ) -> BatchResponses:
        batch_responses = BatchResponses()
        async for datum in datums:
            val = datum.value
            _ = datum.event_time
            _ = datum.watermark
            strs = val.decode("utf-8").split(",")
            batch_response = BatchResponse.from_id(datum.id)
            if len(strs) == 0:
                batch_response.append(Message.to_drop())
            else:
                for s in strs:
                    batch_response.append(Message(str.encode(s)))
            batch_responses.append(batch_response)

        return batch_responses


async def handler(
    datums: AsyncIterable[Datum],
) -> BatchResponses:
    batch_responses = BatchResponses()
    async for datum in datums:
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        strs = val.decode("utf-8").split(",")
        batch_response = BatchResponse.from_id(datum.id)
        if len(strs) == 0:
            batch_response.append(Message.to_drop())
        else:
            for s in strs:
                batch_response.append(Message(str.encode(s)))
        batch_responses.append(batch_response)

    return batch_responses


def NewAsyncBatchMapper():
    d = ExampleClass()
    server_instance = BatchMapAsyncServer(d)
    udfs = server_instance.servicer
    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    return server, listen_addr


@pytest.fixture(scope="module")
def async_batch_map_server():
    """Module-scoped fixture: starts an async gRPC batch map server in a background thread."""
    loop = create_async_loop()

    udfs = NewAsyncBatchMapper()
    server = start_async_server(loop, start_server(udfs))

    yield loop

    teardown_async_server(loop, server)


@pytest.fixture()
def batch_map_stub(async_batch_map_server):
    """Returns a MapStub connected to the running async batch map server."""
    return map_pb2_grpc.MapStub(grpc.insecure_channel(listen_addr))


def test_batch_map(batch_map_stub) -> None:
    generator_response = None

    try:
        generator_response = batch_map_stub.MapFn(
            request_iterator=request_generator(count=10, session=1)
        )
    except grpc.RpcError as e:
        logging.error(e)

    handshake = next(generator_response)
    # assert that handshake response is received.
    assert handshake.handshake.sot
    data_resp = []
    for r in generator_response:
        data_resp.append(r)

    idx = 0
    while idx < len(data_resp) - 1:
        assert (
            bytes(
                "test_mock_message",
                encoding="utf-8",
            )
            == data_resp[idx].results[0].value
        )
        _id = data_resp[idx].id
        assert _id == "test-id-" + str(idx)
        idx += 1
    # EOT Response
    assert data_resp[len(data_resp) - 1].status.eot is True
    # 10 sink responses + 1 EOT response
    assert 11 == len(data_resp)


async def nack_handler(datums: AsyncIterable[Datum]) -> BatchResponses:
    batch_responses = BatchResponses()
    async for datum in datums:
        batch_response = BatchResponse.from_id(datum.id)
        batch_response.append(Message.to_nack(NACK_TEST_OPTIONS))
        batch_responses.append(batch_response)
    return batch_responses


async def start_nack_server(udfs):
    server = grpc.aio.server()
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    server.add_insecure_port(nack_listen_addr)
    logging.info("Starting nack server on %s", nack_listen_addr)
    await server.start()
    return server, nack_listen_addr


@pytest.fixture(scope="module")
def async_nack_batch_map_server():
    loop = create_async_loop()
    udfs = BatchMapAsyncServer(nack_handler).servicer
    server = start_async_server(loop, start_nack_server(udfs))
    yield loop
    teardown_async_server(loop, server)


def test_batch_map_nack(async_nack_batch_map_server) -> None:
    with grpc.insecure_channel(nack_listen_addr) as channel:
        stub = map_pb2_grpc.MapStub(channel)
        generator_response = stub.MapFn(request_iterator=request_generator(count=5, session=1))

        handshake = next(generator_response)
        assert handshake.handshake.sot

        data_resp = [r for r in generator_response]

    # exclude the trailing EOT response
    for resp in data_resp[:-1]:
        result = resp.results[0]
        assert NACK in result.tags
        assert result.nack_options.delay == NACK_TEST_OPTIONS.delay
        assert result.nack_options.max_deliveries == NACK_TEST_OPTIONS.max_deliveries
        assert result.nack_options.reason == NACK_TEST_OPTIONS.reason
    assert data_resp[-1].status.eot is True


async def metadata_handler(datums: AsyncIterable[Datum]) -> BatchResponses:
    """
    Echoes the user metadata, and copies the system metadata version into a
    "system_info" user group so the test can see what the handler received.
    """
    batch_responses = BatchResponses()
    async for datum in datums:
        user_metadata = datum.user_metadata
        version = datum.system_metadata.value("numaflow_version_info", "version")
        if version is not None:
            user_metadata.add_key("system_info", "version", version)
        batch_response = BatchResponse.from_id(datum.id)
        batch_response.append(Message(datum.value, user_metadata=user_metadata))
        batch_responses.append(batch_response)
    return batch_responses


async def start_metadata_server(udfs):
    server = grpc.aio.server()
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    server.add_insecure_port(metadata_listen_addr)
    logging.info("Starting metadata server on %s", metadata_listen_addr)
    await server.start()
    return server, metadata_listen_addr


@pytest.fixture(scope="module")
def async_metadata_batch_map_server():
    loop = create_async_loop()
    udfs = BatchMapAsyncServer(metadata_handler).servicer
    server = start_async_server(loop, start_metadata_server(udfs))
    yield loop
    teardown_async_server(loop, server)


def test_batch_map_metadata(async_metadata_batch_map_server) -> None:
    with grpc.insecure_channel(metadata_listen_addr) as channel:
        stub = map_pb2_grpc.MapStub(channel)
        generator_response = stub.MapFn(request_iterator=metadata_request_generator())

        handshake = next(generator_response)
        assert handshake.handshake.sot

        data_resp = [r for r in generator_response]

    # 2 data responses + 1 EOT response
    assert len(data_resp) == 3
    assert data_resp[-1].status.eot is True

    results = {resp.id: resp.results[0] for resp in data_resp[:-1]}

    with_metadata = results["with-metadata"]
    assert with_metadata.value == b"with-metadata"
    assert {g: dict(kv.key_value) for g, kv in with_metadata.metadata.user_metadata.items()} == {
        "custom_info": {"version": b"1.0.0"},
        "system_info": {"version": b"1.0.0"},
    }
    # System metadata will be empty for user responses
    assert with_metadata.metadata.sys_metadata == {}

    no_metadata = results["no-metadata"]
    assert no_metadata.value == b"no-metadata"
    assert no_metadata.metadata.user_metadata == {}
    assert no_metadata.metadata.sys_metadata == {}


def test_is_ready(async_batch_map_server) -> None:
    with grpc.insecure_channel(listen_addr) as channel:
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
    kwargs = {"batch_mapper_instance": handler}
    if max_threads_arg is not None:
        kwargs["max_threads"] = max_threads_arg
    server = BatchMapAsyncServer(**kwargs)
    assert server.max_threads == expected
