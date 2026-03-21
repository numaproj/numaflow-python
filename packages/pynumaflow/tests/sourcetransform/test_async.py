import asyncio
import logging
import threading

import grpc
import pytest
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import MAX_MESSAGE_SIZE
from pynumaflow.proto.common import metadata_pb2
from pynumaflow.proto.sourcetransformer import transform_pb2_grpc
from pynumaflow.sourcetransformer import Datum, Messages, Message, SourceTransformer
from pynumaflow.sourcetransformer.async_server import SourceTransformAsyncServer
from tests.sourcetransform.utils import get_test_datums
from tests.testing_utils import (
    mock_new_event_time,
)

LOGGER = setup_logging(__name__)

# if set to true, transform handler will raise a `ValueError` exception.
raise_error_from_st = False

SOCK_PATH = "unix:///tmp/async_st.sock"
METADATA_SOCK_PATH = "unix:///tmp/async_st_metadata.sock"


class SimpleAsyncSourceTrn(SourceTransformer):
    async def handler(self, keys: list[str], datum: Datum) -> Messages:
        if raise_error_from_st:
            raise ValueError("Exception thrown from transform")
        val = datum.value
        msg = "payload:{} event_time:{} ".format(
            val.decode("utf-8"),
            datum.event_time,
        )
        val = bytes(msg, encoding="utf-8")
        messages = Messages()
        messages.append(Message(val, mock_new_event_time(), keys=keys))
        return messages


def request_generator(req):
    yield from req


def _startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def _start_server(udfs):
    _server_options = [
        ("grpc.max_send_message_length", MAX_MESSAGE_SIZE),
        ("grpc.max_receive_message_length", MAX_MESSAGE_SIZE),
    ]
    server = grpc.aio.server(options=_server_options)
    transform_pb2_grpc.add_SourceTransformServicer_to_server(udfs, server)
    server.add_insecure_port(SOCK_PATH)
    logging.info("Starting server on %s", SOCK_PATH)
    await server.start()
    return server


@pytest.fixture(scope="module")
def async_st_server():
    """Module-scoped fixture: starts an async gRPC source transform server."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=_startup_callable, args=(loop,), daemon=True)
    thread.start()

    handle = SimpleAsyncSourceTrn()
    server_obj = SourceTransformAsyncServer(source_transform_instance=handle)
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
def st_stub(async_st_server):
    """Returns a SourceTransformStub connected to the running async server."""
    return transform_pb2_grpc.SourceTransformStub(grpc.insecure_channel(SOCK_PATH))


def test_run_server(async_st_server):
    with grpc.insecure_channel(SOCK_PATH) as channel:
        stub = transform_pb2_grpc.SourceTransformStub(channel)
        request = get_test_datums()
        generator_response = None
        try:
            generator_response = stub.SourceTransformFn(request_iterator=request_generator(request))
        except grpc.RpcError as e:
            logging.error(e)

        responses = []
        # capture the output from the ReadFn generator and assert.
        for r in generator_response:
            responses.append(r)

        # 1 handshake + 3 data responses
        assert len(responses) == 4

        assert responses[0].handshake.sot

        idx = 1
        while idx < len(responses):
            _id = "test-id-" + str(idx)
            assert responses[idx].id == _id
            assert responses[idx].results[0].value == bytes(
                "payload:test_mock_message " "event_time:2022-09-12 16:00:00 ",
                encoding="utf-8",
            )
            assert len(responses[idx].results) == 1
            idx += 1

        LOGGER.info("Successfully validated the server")


def test_async_source_transformer(st_stub):
    request = get_test_datums()
    generator_response = None
    try:
        generator_response = st_stub.SourceTransformFn(request_iterator=request_generator(request))
    except grpc.RpcError as e:
        logging.error(e)

    responses = []
    # capture the output from the ReadFn generator and assert.
    for r in generator_response:
        responses.append(r)

    # 1 handshake + 3 data responses
    assert len(responses) == 4

    assert responses[0].handshake.sot

    idx = 1
    while idx < len(responses):
        _id = "test-id-" + str(idx)
        assert responses[idx].id == _id
        assert responses[idx].results[0].value == bytes(
            "payload:test_mock_message " "event_time:2022-09-12 16:00:00 ",
            encoding="utf-8",
        )
        assert len(responses[idx].results) == 1
        idx += 1

    # Verify new event time gets assigned.
    updated_event_time_timestamp = _timestamp_pb2.Timestamp()
    updated_event_time_timestamp.FromDatetime(dt=mock_new_event_time())
    assert responses[1].results[0].event_time == updated_event_time_timestamp


def test_async_source_transformer_grpc_error_no_handshake(st_stub):
    request = get_test_datums(handshake=False)
    grpc_exception = None

    responses = []
    try:
        generator_response = st_stub.SourceTransformFn(request_iterator=request_generator(request))
        # capture the output from the ReadFn generator and assert.
        for r in generator_response:
            responses.append(r)
    except grpc.RpcError as e:
        logging.error(e)
        grpc_exception = e
        assert "SourceTransformFn: expected handshake message" in str(e)

    assert len(responses) == 0
    assert grpc_exception is not None


def test_async_source_transformer_grpc_error(st_stub):
    request = get_test_datums()
    grpc_exception = None

    responses = []
    try:
        global raise_error_from_st
        raise_error_from_st = True
        generator_response = st_stub.SourceTransformFn(request_iterator=request_generator(request))
        # capture the output from the ReadFn generator and assert.
        for r in generator_response:
            responses.append(r)
    except grpc.RpcError as e:
        logging.error(e)
        grpc_exception = e
        assert e.code() == grpc.StatusCode.INTERNAL
        assert "Exception thrown from transform" in str(e)
    finally:
        raise_error_from_st = False
    # 1 handshake
    assert len(responses) == 1
    assert grpc_exception is not None


def test_is_ready(async_st_server):
    with grpc.insecure_channel(SOCK_PATH) as channel:
        stub = transform_pb2_grpc.SourceTransformStub(channel)

        request = _empty_pb2.Empty()
        response = None
        try:
            response = stub.IsReady(request=request)
        except grpc.RpcError as e:
            logging.error(e)

        assert response.ready


def test_invalid_input():
    with pytest.raises(TypeError):
        SourceTransformAsyncServer()


@pytest.mark.parametrize(
    "max_threads_arg,expected",
    [
        (32, 16),  # max cap at 16
        (5, 5),  # use argument provided
        (None, 4),  # defaults to 4
    ],
)
def test_max_threads(max_threads_arg, expected):
    handle = SimpleAsyncSourceTrn()
    kwargs = {"source_transform_instance": handle}
    if max_threads_arg is not None:
        kwargs["max_threads"] = max_threads_arg
    server = SourceTransformAsyncServer(**kwargs)
    assert server.max_threads == expected


# --- Metadata test class ---


class MetadataAsyncSourceTransformer(SourceTransformer):
    """Source transformer that validates and passes through metadata."""

    async def handler(self, keys: list[str], datum: Datum) -> Messages:
        # Validate system metadata
        if datum.system_metadata.value("numaflow_version_info", "version") != b"1.0.0":
            raise ValueError("System metadata version mismatch")

        val = datum.value
        msg = "payload:{} event_time:{} ".format(
            val.decode("utf-8"),
            datum.event_time,
        )
        val = bytes(msg, encoding="utf-8")
        messages = Messages()
        # Pass user metadata to the output message
        messages.append(
            Message(val, mock_new_event_time(), keys=keys, user_metadata=datum.user_metadata)
        )
        return messages


async def _start_metadata_server(udfs):
    _server_options = [
        ("grpc.max_send_message_length", MAX_MESSAGE_SIZE),
        ("grpc.max_receive_message_length", MAX_MESSAGE_SIZE),
    ]
    server = grpc.aio.server(options=_server_options)
    transform_pb2_grpc.add_SourceTransformServicer_to_server(udfs, server)
    server.add_insecure_port(METADATA_SOCK_PATH)
    logging.info("Starting metadata server on %s", METADATA_SOCK_PATH)
    await server.start()
    return server


@pytest.fixture(scope="module")
def async_st_metadata_server():
    """Module-scoped fixture: starts an async gRPC metadata source transform server."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=_startup_callable, args=(loop,), daemon=True)
    thread.start()

    handle = MetadataAsyncSourceTransformer()
    server_obj = SourceTransformAsyncServer(source_transform_instance=handle)
    udfs = server_obj.servicer
    future = asyncio.run_coroutine_threadsafe(_start_metadata_server(udfs), loop=loop)
    future.result(timeout=10)

    while True:
        try:
            with grpc.insecure_channel(METADATA_SOCK_PATH) as channel:
                f = grpc.channel_ready_future(channel)
                f.result(timeout=10)
                if f.done():
                    break
        except grpc.FutureTimeoutError as e:
            LOGGER.error("error trying to connect to grpc server")
            LOGGER.error(e)

    yield loop

    loop.stop()
    LOGGER.info("stopped the metadata event loop")


@pytest.fixture()
def metadata_stub(async_st_metadata_server):
    """Returns a SourceTransformStub connected to the metadata server."""
    return transform_pb2_grpc.SourceTransformStub(grpc.insecure_channel(METADATA_SOCK_PATH))


def test_source_transformer_with_metadata(metadata_stub):
    request = get_test_datums(with_metadata=True)
    generator_response = None
    try:
        generator_response = metadata_stub.SourceTransformFn(
            request_iterator=request_generator(request)
        )
    except grpc.RpcError as e:
        logging.error(e)
        raise

    responses = []
    for r in generator_response:
        responses.append(r)

    # 1 handshake + 3 data responses
    assert len(responses) == 4
    assert responses[0].handshake.sot

    # Verify metadata is passed through correctly
    for idx, resp in enumerate(responses[1:], 1):
        _id = "test-id-" + str(idx)
        assert resp.id == _id
        assert len(resp.results) == 1
        # Verify user metadata is returned
        assert resp.results[0].metadata.user_metadata["custom_info"] == metadata_pb2.KeyValueGroup(
            key_value={"version": f"{idx}.0.0".encode()}
        )
        # System metadata should be empty in responses (user cannot set it)
        assert resp.results[0].metadata.sys_metadata == {}
