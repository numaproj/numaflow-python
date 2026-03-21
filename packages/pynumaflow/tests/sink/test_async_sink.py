import asyncio
import logging
import threading
from collections.abc import AsyncIterable

import grpc
import pytest
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    ERR_UDF_EXCEPTION_STRING,
    UD_CONTAINER_FALLBACK_SINK,
    FALLBACK_SINK_SOCK_PATH,
    FALLBACK_SINK_SERVER_INFO_FILE_PATH,
    UD_CONTAINER_ON_SUCCESS_SINK,
    ON_SUCCESS_SINK_SOCK_PATH,
    ON_SUCCESS_SINK_SERVER_INFO_FILE_PATH,
)
from pynumaflow.proto.common import metadata_pb2
from pynumaflow.sinker import (
    Datum,
)
from pynumaflow.sinker import Responses, Response, Message, UserMetadata
from pynumaflow.proto.sinker import sink_pb2_grpc, sink_pb2
from pynumaflow.sinker.async_server import SinkAsyncServer
from tests.sink.test_server import (
    mock_message,
    mock_err_message,
    mock_fallback_message,
    mockenv,
)
from tests.testing_utils import get_time_args

LOGGER = setup_logging(__name__)

SOCK_PATH = "unix:///tmp/async_sink.sock"


async def udsink_handler(datums: AsyncIterable[Datum]) -> Responses:
    responses = Responses()
    async for msg in datums:
        if msg.value.decode("utf-8") == "test_mock_err_message":
            raise ValueError("test_mock_err_message")
        elif msg.value.decode("utf-8") == "test_mock_fallback_message":
            responses.append(Response.as_fallback(msg.id))
        elif msg.value.decode("utf-8") == "test_mock_on_success1_message":
            responses.append(Response.as_on_success(msg.id, None))
        elif msg.value.decode("utf-8") == "test_mock_on_success2_message":
            responses.append(
                Response.as_on_success(msg.id, Message(b"value", ["key"], UserMetadata()))
            )
        else:
            if msg.user_metadata.groups() != ["custom_info"]:
                raise ValueError("user metadata groups do not match")
            if msg.system_metadata.groups() != ["numaflow_version_info"]:
                raise ValueError("system metadata groups do not match")
            responses.append(Response.as_success(msg.id))
    return responses


def start_sink_streaming_request(_id: str, req_type) -> (Datum, tuple):
    event_time_timestamp, watermark_timestamp = get_time_args()
    value = mock_message()
    if req_type == "err":
        value = mock_err_message()

    if req_type == "fallback":
        value = mock_fallback_message()

    if req_type == "on_success1":
        value = b"test_mock_on_success1_message"

    if req_type == "on_success2":
        value = b"test_mock_on_success2_message"

    request = sink_pb2.SinkRequest.Request(
        value=value,
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
        id=_id,
        metadata=metadata_pb2.Metadata(
            previous_vertex="test-source",
            user_metadata={
                "custom_info": metadata_pb2.KeyValueGroup(key_value={"version": b"1.0.0"}),
            },
            sys_metadata={
                "numaflow_version_info": metadata_pb2.KeyValueGroup(
                    key_value={"version": b"1.0.0"}
                ),
            },
        ),
    )
    return sink_pb2.SinkRequest(request=request)


def request_generator(count, req_type="success", session=1, handshake=True):
    if handshake:
        yield sink_pb2.SinkRequest(handshake=sink_pb2.Handshake(sot=True))

    for _ in range(session):
        yield from (start_sink_streaming_request(str(i), req_type) for i in range(count))
        yield sink_pb2.SinkRequest(status=sink_pb2.TransmissionStatus(eot=True))


def _startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def _start_server():
    server = grpc.aio.server()
    server_instance = SinkAsyncServer(sinker_instance=udsink_handler)
    uds = server_instance.servicer
    sink_pb2_grpc.add_SinkServicer_to_server(uds, server)
    server.add_insecure_port(SOCK_PATH)
    logging.info("Starting server on %s", SOCK_PATH)
    await server.start()
    return server


@pytest.fixture(scope="module")
def async_sink_server():
    """Module-scoped fixture: starts an async gRPC sink server in a background thread."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=_startup_callable, args=(loop,), daemon=True)
    thread.start()

    future = asyncio.run_coroutine_threadsafe(_start_server(), loop=loop)
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
def sink_stub(async_sink_server):
    """Returns a SinkStub connected to the running async server."""
    return sink_pb2_grpc.SinkStub(grpc.insecure_channel(SOCK_PATH))


def test_run_server(async_sink_server):
    with grpc.insecure_channel(SOCK_PATH) as channel:
        stub = sink_pb2_grpc.SinkStub(channel)

        request = _empty_pb2.Empty()
        response = None
        try:
            response = stub.IsReady(request=request)
        except grpc.RpcError as e:
            logging.error(e)

        assert response.ready


def test_sink(sink_stub):
    generator_response = None
    grpc_exception = None
    try:
        generator_response = sink_stub.SinkFn(
            request_iterator=request_generator(count=10, req_type="success", session=1)
        )
        handshake = next(generator_response)
        # assert that handshake response is received.
        assert handshake.handshake.sot

        data_resp = []
        for r in generator_response:
            data_resp.append(r)

        # 1 sink data response + 1 EOT response
        assert len(data_resp) == 2

        idx = 0
        # capture the output from the SinkFn generator and assert.
        for resp in data_resp[0].results:
            assert resp.id == str(idx)
            assert resp.status == sink_pb2.Status.SUCCESS
            idx += 1
        # EOT Response
        assert data_resp[1].status.eot is True

    except grpc.RpcError as e:
        logging.error(e)
        grpc_exception = e

    assert grpc_exception is None


def test_sink_err(sink_stub):
    grpc_exception = None
    try:
        generator_response = sink_stub.SinkFn(
            request_iterator=request_generator(count=10, req_type="err")
        )
        for _ in generator_response:
            pass
    except BaseException as e:
        assert f"{ERR_UDF_EXCEPTION_STRING}: ValueError('test_mock_err_message')" in str(e)
        return
    except grpc.RpcError as e:
        grpc_exception = e
        assert e.code() == grpc.StatusCode.UNKNOWN
        print(e.details())

    assert grpc_exception is not None


def test_sink_err_handshake(sink_stub):
    grpc_exception = None
    try:
        generator_response = sink_stub.SinkFn(
            request_iterator=request_generator(count=10, req_type="success", handshake=False)
        )
        for _ in generator_response:
            pass
    except BaseException as e:
        assert "ReadFn: expected handshake message" in str(e)
        return
    except grpc.RpcError as e:
        grpc_exception = e
        assert e.code() == grpc.StatusCode.UNKNOWN
        print(e.details())

    assert grpc_exception is not None


def test_sink_fallback(sink_stub):
    try:
        generator_response = sink_stub.SinkFn(
            request_iterator=request_generator(count=10, req_type="fallback", session=1)
        )
        handshake = next(generator_response)
        # assert that handshake response is received.
        assert handshake.handshake.sot

        data_resp = []
        for r in generator_response:
            data_resp.append(r)

        # 1 sink data response + 1 EOT response
        assert len(data_resp) == 2

        idx = 0
        # capture the output from the SinkFn generator and assert.
        for resp in data_resp[0].results:
            assert resp.id == str(idx)
            assert resp.status == sink_pb2.Status.FALLBACK
            idx += 1
        # EOT Response
        assert data_resp[1].status.eot is True

    except grpc.RpcError as e:
        logging.error(e)


def test_sink_on_success1(sink_stub):
    grpc_exception = None
    try:
        generator_response = sink_stub.SinkFn(
            request_iterator=request_generator(count=10, req_type="on_success1", session=1)
        )
        handshake = next(generator_response)
        # assert that handshake response is received.
        assert handshake.handshake.sot

        data_resp = []
        for r in generator_response:
            data_resp.append(r)

        # 1 sink data response + 1 EOT response
        assert len(data_resp) == 2

        idx = 0
        # capture the output from the SinkFn generator and assert.
        for resp in data_resp[0].results:
            assert resp.id == str(idx)
            assert resp.status == sink_pb2.Status.ON_SUCCESS
            idx += 1
        # EOT Response
        assert data_resp[1].status.eot is True

    except grpc.RpcError as e:
        logging.error(e)
        grpc_exception = e

    assert grpc_exception is None


def test_sink_on_success2(sink_stub):
    grpc_exception = None
    try:
        generator_response = sink_stub.SinkFn(
            request_iterator=request_generator(count=10, req_type="on_success2", session=1)
        )
        handshake = next(generator_response)
        # assert that handshake response is received.
        assert handshake.handshake.sot

        data_resp = []
        for r in generator_response:
            data_resp.append(r)

        # 1 sink data response + 1 EOT response
        assert len(data_resp) == 2

        idx = 0
        # capture the output from the SinkFn generator and assert.
        for resp in data_resp[0].results:
            assert resp.id == str(idx)
            assert resp.status == sink_pb2.Status.ON_SUCCESS
            idx += 1
        # EOT Response
        assert data_resp[1].status.eot is True

    except grpc.RpcError as e:
        logging.error(e)
        grpc_exception = e

    assert grpc_exception is None


def test_invalid_server_type():
    with pytest.raises(TypeError):
        SinkAsyncServer()


@mockenv(NUMAFLOW_UD_CONTAINER_TYPE=UD_CONTAINER_FALLBACK_SINK)
def test_start_fallback_sink():
    server = SinkAsyncServer(sinker_instance=udsink_handler)
    assert server.sock_path == f"unix://{FALLBACK_SINK_SOCK_PATH}"
    assert server.server_info_file == FALLBACK_SINK_SERVER_INFO_FILE_PATH


@mockenv(NUMAFLOW_UD_CONTAINER_TYPE=UD_CONTAINER_ON_SUCCESS_SINK)
def test_start_on_success_sink():
    server = SinkAsyncServer(sinker_instance=udsink_handler)
    assert server.sock_path == f"unix://{ON_SUCCESS_SINK_SOCK_PATH}"
    assert server.server_info_file == ON_SUCCESS_SINK_SERVER_INFO_FILE_PATH


@pytest.mark.parametrize(
    "max_threads_arg,expected",
    [
        (32, 16),  # max cap at 16
        (5, 5),  # use argument provided
        (None, 4),  # defaults to 4
    ],
)
def test_max_threads(max_threads_arg, expected):
    kwargs = {"sinker_instance": udsink_handler}
    if max_threads_arg is not None:
        kwargs["max_threads"] = max_threads_arg
    server = SinkAsyncServer(**kwargs)
    assert server.max_threads == expected
