import os
from collections.abc import Iterator
from datetime import datetime, timezone
from unittest import mock

import grpc
import pytest
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow._constants import (
    UD_CONTAINER_FALLBACK_SINK,
    FALLBACK_SINK_SOCK_PATH,
    FALLBACK_SINK_SERVER_INFO_FILE_PATH,
    UD_CONTAINER_ON_SUCCESS_SINK,
    ON_SUCCESS_SINK_SOCK_PATH,
    ON_SUCCESS_SINK_SERVER_INFO_FILE_PATH,
)
from pynumaflow.proto.common import metadata_pb2
from pynumaflow.proto.sinker import sink_pb2
from pynumaflow.sinker import Responses, Datum, Response, SinkServer, Message, UserMetadata
from pynumaflow.sinker.servicer.sync_servicer import SyncSinkServicer


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


def udsink_handler(datums: Iterator[Datum]) -> Responses:
    results = Responses()
    for msg in datums:
        if "err" in msg.value.decode("utf-8"):
            results.append(Response.as_failure(msg.id, "mock sink message error"))
        elif "fallback" in msg.value.decode("utf-8"):
            results.append(Response.as_fallback(msg.id))
        elif "on_success1" in msg.value.decode("utf-8"):
            results.append(Response.as_on_success(msg.id, None))
        elif "on_success2" in msg.value.decode("utf-8"):
            results.append(
                Response.as_on_success(msg.id, Message(b"value", ["key"], UserMetadata()))
            )
        else:
            if msg.user_metadata.groups() != ["custom_info"]:
                raise ValueError("user metadata groups do not match")
            if msg.system_metadata.groups() != ["numaflow_version_info"]:
                raise ValueError("system metadata groups do not match")
            results.append(Response.as_success(msg.id))
    return results


def err_udsink_handler(_: Iterator[Datum]) -> Responses:
    raise RuntimeError("Something is fishy!")


def mock_message():
    msg = bytes("test_mock_message", encoding="utf-8")
    return msg


def mock_err_message():
    msg = bytes("test_mock_err_message", encoding="utf-8")
    return msg


def mock_fallback_message():
    msg = bytes("test_mock_fallback_message", encoding="utf-8")
    return msg


def mock_event_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


def mock_watermark():
    t = datetime.fromtimestamp(1662998460, timezone.utc)
    return t


METADATA = metadata_pb2.Metadata(
    previous_vertex="test-source",
    user_metadata={
        "custom_info": metadata_pb2.KeyValueGroup(key_value={"version": b"1.0.0"}),
    },
    sys_metadata={
        "numaflow_version_info": metadata_pb2.KeyValueGroup(key_value={"version": b"1.0.0"}),
    },
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sink_test_server():
    server = SinkServer(sinker_instance=udsink_handler)
    services = {sink_pb2.DESCRIPTOR.services_by_name["Sink"]: server.servicer}
    return server_from_dictionary(services, strict_real_time())


@pytest.fixture()
def err_sink_test_server():
    server = SinkServer(sinker_instance=err_udsink_handler)
    return server, server_from_dictionary(
        {sink_pb2.DESCRIPTOR.services_by_name["Sink"]: server.servicer},
        strict_real_time(),
    )


def _make_timestamps():
    event_time_timestamp = _timestamp_pb2.Timestamp()
    event_time_timestamp.FromDatetime(dt=mock_event_time())
    watermark_timestamp = _timestamp_pb2.Timestamp()
    watermark_timestamp.FromDatetime(dt=mock_watermark())
    return event_time_timestamp, watermark_timestamp


# ---------------------------------------------------------------------------
# Server tests
# ---------------------------------------------------------------------------


def test_is_ready(sink_test_server):
    method = sink_test_server.invoke_unary_unary(
        method_descriptor=(sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["IsReady"]),
        invocation_metadata={},
        request=_empty_pb2.Empty(),
        timeout=1,
    )

    response, metadata, code, details = method.termination()
    expected = sink_pb2.ReadyResponse(ready=True)
    assert response == expected
    assert code == StatusCode.OK


def test_udsink_err_handshake(err_sink_test_server):
    _, test_server = err_sink_test_server
    event_time_timestamp, watermark_timestamp = _make_timestamps()

    test_datums = [
        sink_pb2.SinkRequest(
            request=sink_pb2.SinkRequest.Request(
                id="test_id_0",
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
            )
        ),
        sink_pb2.SinkRequest(status=sink_pb2.TransmissionStatus(eot=True)),
    ]

    method = test_server.invoke_stream_stream(
        method_descriptor=(sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["SinkFn"]),
        invocation_metadata={},
        timeout=1,
    )

    method.send_request(test_datums[0])

    metadata, code, details = method.termination()
    assert "UDSinkError" in details
    assert "SinkFn: expected handshake message" in details
    assert code == StatusCode.INTERNAL


def test_udsink_err(err_sink_test_server):
    _, test_server = err_sink_test_server
    event_time_timestamp, watermark_timestamp = _make_timestamps()

    test_datums = [
        sink_pb2.SinkRequest(handshake=sink_pb2.Handshake(sot=True)),
        sink_pb2.SinkRequest(
            request=sink_pb2.SinkRequest.Request(
                id="test_id_0",
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                metadata=METADATA,
            )
        ),
        sink_pb2.SinkRequest(
            request=sink_pb2.SinkRequest.Request(
                id="test_id_1",
                value=mock_err_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                metadata=METADATA,
            )
        ),
        sink_pb2.SinkRequest(status=sink_pb2.TransmissionStatus(eot=True)),
    ]

    method = test_server.invoke_stream_stream(
        method_descriptor=(sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["SinkFn"]),
        invocation_metadata={},
        timeout=1,
    )

    for d in test_datums:
        method.send_request(d)
    method.requests_closed()

    responses = []
    while True:
        try:
            resp = method.take_response()
            responses.append(resp)
        except ValueError as err:
            if "No more responses!" in str(err):
                break

    metadata, code, details = method.termination()
    assert code == StatusCode.INTERNAL


def test_forward_message(sink_test_server):
    event_time_timestamp, watermark_timestamp = _make_timestamps()

    test_datums = [
        sink_pb2.SinkRequest(handshake=sink_pb2.Handshake(sot=True)),
        sink_pb2.SinkRequest(
            request=sink_pb2.SinkRequest.Request(
                id="test_id_0",
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                metadata=METADATA,
            )
        ),
        sink_pb2.SinkRequest(
            request=sink_pb2.SinkRequest.Request(
                id="test_id_1",
                value=mock_err_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                metadata=METADATA,
            )
        ),
        sink_pb2.SinkRequest(
            request=sink_pb2.SinkRequest.Request(
                id="test_id_2",
                value=b"test_mock_on_success1_message",
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                metadata=METADATA,
            )
        ),
        sink_pb2.SinkRequest(
            request=sink_pb2.SinkRequest.Request(
                id="test_id_3",
                value=b"test_mock_on_success2_message",
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                metadata=METADATA,
            )
        ),
        sink_pb2.SinkRequest(status=sink_pb2.TransmissionStatus(eot=True)),
    ]

    method = sink_test_server.invoke_stream_stream(
        method_descriptor=(sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["SinkFn"]),
        invocation_metadata={},
        timeout=1,
    )
    for x in test_datums:
        method.send_request(x)
    method.requests_closed()

    responses = []
    while True:
        try:
            resp = method.take_response()
            responses.append(resp)
        except ValueError as err:
            if "No more responses!" in str(err):
                break

    # 1 handshake +  1 data messages + 1 EOT
    assert len(responses) == 3
    # first message should be handshake response
    assert responses[0].handshake.sot

    assert len(responses[1].results) == 4

    # assert the values for the corresponding messages
    assert responses[1].results[0].id == "test_id_0"
    assert responses[1].results[1].id == "test_id_1"
    assert responses[1].results[2].id == "test_id_2"
    assert responses[1].results[3].id == "test_id_3"
    assert responses[1].results[0].status == sink_pb2.Status.SUCCESS
    assert responses[1].results[1].status == sink_pb2.Status.FAILURE
    assert responses[1].results[0].err_msg == ""
    assert responses[1].results[1].err_msg == "mock sink message error"

    # last message should be EOT response
    assert responses[2].status.eot

    _, code, _ = method.termination()
    assert code == StatusCode.OK


def test_invalid_init():
    with pytest.raises(TypeError):
        SinkServer()


@mockenv(NUMAFLOW_UD_CONTAINER_TYPE=UD_CONTAINER_FALLBACK_SINK)
def test_start_fallback_sink():
    server = SinkServer(sinker_instance=udsink_handler)
    assert server.sock_path == f"unix://{FALLBACK_SINK_SOCK_PATH}"
    assert server.server_info_file == FALLBACK_SINK_SERVER_INFO_FILE_PATH


@mockenv(NUMAFLOW_UD_CONTAINER_TYPE=UD_CONTAINER_ON_SUCCESS_SINK)
def test_start_on_success_sink():
    server = SinkServer(sinker_instance=udsink_handler)
    assert server.sock_path == f"unix://{ON_SUCCESS_SINK_SOCK_PATH}"
    assert server.server_info_file == ON_SUCCESS_SINK_SERVER_INFO_FILE_PATH


def test_max_threads():
    # max cap at 16
    server = SinkServer(sinker_instance=udsink_handler, max_threads=32)
    assert server.max_threads == 16

    # use argument provided
    server = SinkServer(sinker_instance=udsink_handler, max_threads=5)
    assert server.max_threads == 5

    # defaults to 4
    server = SinkServer(sinker_instance=udsink_handler)
    assert server.max_threads == 4


# ---------------------------------------------------------------------------
# Shutdown-event tests
# ---------------------------------------------------------------------------


def test_shutdown_event_set_on_handler_error():
    """When the UDF handler raises, the servicer must signal the shutdown event."""
    servicer = SyncSinkServicer(handler=err_udsink_handler)
    event_time_timestamp, watermark_timestamp = _make_timestamps()

    services = {sink_pb2.DESCRIPTOR.services_by_name["Sink"]: servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    test_datums = [
        sink_pb2.SinkRequest(handshake=sink_pb2.Handshake(sot=True)),
        sink_pb2.SinkRequest(
            request=sink_pb2.SinkRequest.Request(
                id="test_id_0",
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                metadata=METADATA,
            )
        ),
        sink_pb2.SinkRequest(status=sink_pb2.TransmissionStatus(eot=True)),
    ]

    method = test_server.invoke_stream_stream(
        method_descriptor=(sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["SinkFn"]),
        invocation_metadata={},
        timeout=2,
    )

    for d in test_datums:
        method.send_request(d)
    method.requests_closed()

    while True:
        try:
            method.take_response()
        except ValueError:
            break

    _, code, _ = method.termination()
    assert code == StatusCode.INTERNAL
    assert servicer.shutdown_event.is_set()
    assert servicer.error is not None


def test_shutdown_event_set_on_handshake_error():
    """Missing handshake must also signal the shutdown event."""
    servicer = SyncSinkServicer(handler=udsink_handler)
    event_time_timestamp, watermark_timestamp = _make_timestamps()

    services = {sink_pb2.DESCRIPTOR.services_by_name["Sink"]: servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    method = test_server.invoke_stream_stream(
        method_descriptor=(sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["SinkFn"]),
        invocation_metadata={},
        timeout=1,
    )

    method.send_request(
        sink_pb2.SinkRequest(
            request=sink_pb2.SinkRequest.Request(
                id="test_id_0",
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
            )
        )
    )

    _, code, details = method.termination()
    assert code == StatusCode.INTERNAL
    assert "SinkFn: expected handshake message" in details
    assert servicer.shutdown_event.is_set()
    assert servicer.error is not None


def test_shutdown_event_set_on_stream_close_before_handshake():
    """grpc.RpcError on the first read (before handshake): shutdown_event set, req_queue is None so close is skipped."""
    servicer = SyncSinkServicer(handler=udsink_handler)

    def _cancelled_iter():
        raise grpc.RpcError()
        yield  # make it a generator

    responses = list(servicer.SinkFn(_cancelled_iter(), mock.MagicMock()))

    assert responses == []
    assert servicer.shutdown_event.is_set()
    assert servicer.error is None


def test_shutdown_event_set_on_stream_close_mid_batch():
    """grpc.RpcError mid-batch: req_queue is closed (unblocking the handler thread) and shutdown_event is set."""
    servicer = SyncSinkServicer(handler=udsink_handler)
    event_time_timestamp, watermark_timestamp = _make_timestamps()

    def _cancelled_iter():
        yield sink_pb2.SinkRequest(handshake=sink_pb2.Handshake(sot=True))
        yield sink_pb2.SinkRequest(
            request=sink_pb2.SinkRequest.Request(
                id="test_id_0",
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
                metadata=METADATA,
            )
        )
        raise grpc.RpcError()

    responses = list(servicer.SinkFn(_cancelled_iter(), mock.MagicMock()))

    assert responses[0].handshake.sot
    assert servicer.shutdown_event.is_set()
    assert servicer.error is None
