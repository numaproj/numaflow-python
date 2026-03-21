import pytest
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.proto.common import metadata_pb2
from pynumaflow.proto.sourcetransformer import transform_pb2
from pynumaflow.sourcetransformer import SourceTransformServer, Datum, Messages, Message
from tests.sourcetransform.utils import transform_handler, err_transform_handler, get_test_datums
from tests.conftest import collect_responses, drain_responses, send_test_requests
from tests.testing_utils import mock_new_event_time


def _make_transform_server(handler):
    server = SourceTransformServer(source_transform_instance=handler)
    services = {transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"]: server.servicer}
    return server_from_dictionary(services, strict_real_time())


def _invoke_transform_fn(test_server, timeout=1):
    """Helper to invoke the SourceTransformFn stream method."""
    return test_server.invoke_stream_stream(
        method_descriptor=(
            transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"].methods_by_name[
                "SourceTransformFn"
            ]
        ),
        invocation_metadata={},
        timeout=timeout,
    )


@pytest.fixture()
def transform_test_server():
    return _make_transform_server(transform_handler)


# ---------------------------------------------------------------------------
# TestServer tests
# ---------------------------------------------------------------------------


def test_init_with_args():
    server = SourceTransformServer(
        source_transform_instance=transform_handler,
        sock_path="/tmp/test.sock",
        max_message_size=1024 * 1024 * 5,
    )
    assert server.sock_path == "unix:///tmp/test.sock"
    assert server.max_message_size == 1024 * 1024 * 5


@pytest.mark.parametrize(
    "handshake,expected_msg",
    [
        (True, "Something is fishy"),
        (False, "SourceTransformFn: expected handshake message"),
    ],
)
def test_udf_mapt_error(handshake, expected_msg):
    test_server = _make_transform_server(err_transform_handler)
    test_datums = get_test_datums(handshake=handshake)
    method = _invoke_transform_fn(test_server)

    send_test_requests(method, test_datums)
    drain_responses(method)

    metadata, code, details = method.termination()
    assert expected_msg in details
    assert code == StatusCode.INTERNAL


def test_is_ready(transform_test_server):
    method = transform_test_server.invoke_unary_unary(
        method_descriptor=(
            transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"].methods_by_name["IsReady"]
        ),
        invocation_metadata={},
        request=_empty_pb2.Empty(),
        timeout=1,
    )

    response, metadata, code, details = method.termination()
    assert response == transform_pb2.ReadyResponse(ready=True)
    assert code == StatusCode.OK


def test_mapt_assign_new_event_time(transform_test_server):
    test_datums = get_test_datums()
    method = _invoke_transform_fn(transform_test_server)

    send_test_requests(method, test_datums)
    responses = collect_responses(method)

    metadata, code, details = method.termination()

    # 1 handshake + 3 data responses
    assert len(responses) == 4
    assert responses[0].handshake.sot

    result_ids = {f"test-id-{id}" for id in range(1, 4)}
    idx = 1
    while idx < len(responses):
        result_ids.remove(responses[idx].id)
        assert responses[idx].results[0].value == bytes(
            "payload:test_mock_message event_time:2022-09-12 16:00:00 ",
            encoding="utf-8",
        )
        assert len(responses[idx].results) == 1
        idx += 1
    assert len(result_ids) == 0

    # Verify new event time gets assigned.
    updated_event_time_timestamp = _timestamp_pb2.Timestamp()
    updated_event_time_timestamp.FromDatetime(dt=mock_new_event_time())
    assert responses[1].results[0].event_time == updated_event_time_timestamp
    assert code == StatusCode.OK


def test_invalid_input():
    with pytest.raises(TypeError):
        SourceTransformServer()


@pytest.mark.parametrize(
    "max_threads_arg,expected",
    [
        (32, 16),  # max cap at 16
        (5, 5),  # use argument provided
        (None, 4),  # defaults to 4
    ],
)
def test_max_threads(max_threads_arg, expected):
    kwargs = {"source_transform_instance": transform_handler}
    if max_threads_arg is not None:
        kwargs["max_threads"] = max_threads_arg
    server = SourceTransformServer(**kwargs)
    assert server.max_threads == expected


# ---------------------------------------------------------------------------
# Metadata tests
# ---------------------------------------------------------------------------


def metadata_transform_handler(keys: list[str], datum: Datum) -> Messages:
    """Handler that validates system metadata and passes through user metadata."""
    if datum.system_metadata.value("numaflow_version_info", "version") != b"1.0.0":
        raise ValueError("System metadata version mismatch")

    val = datum.value
    msg = "payload:{} event_time:{} ".format(
        val.decode("utf-8"),
        datum.event_time,
    )
    val = bytes(msg, encoding="utf-8")
    messages = Messages()
    messages.append(
        Message(val, mock_new_event_time(), keys=keys, user_metadata=datum.user_metadata)
    )
    return messages


@pytest.fixture()
def metadata_test_server():
    return _make_transform_server(metadata_transform_handler)


def test_source_transform_with_metadata(metadata_test_server):
    test_datums = get_test_datums(with_metadata=True)
    method = _invoke_transform_fn(metadata_test_server)

    send_test_requests(method, test_datums)
    responses = collect_responses(method)

    metadata, code, details = method.termination()

    # 1 handshake + 3 data responses
    assert len(responses) == 4
    assert responses[0].handshake.sot

    # Verify metadata is passed through correctly
    result_metadata = {}
    for resp in responses[1:]:
        result_metadata[resp.id] = resp.results[0].metadata

    for idx in range(1, 4):
        _id = f"test-id-{idx}"
        assert _id in result_metadata
        assert result_metadata[_id].user_metadata["custom_info"] == metadata_pb2.KeyValueGroup(
            key_value={"version": f"{idx}.0.0".encode()}
        )
        # System metadata should be empty in responses
        assert result_metadata[_id].sys_metadata == {}

    assert code == StatusCode.OK
