import pytest
from google.protobuf import empty_pb2 as _empty_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.mapper import MapServer
from pynumaflow.proto.mapper import map_pb2
from tests.map.utils import map_handler, err_map_handler, ExampleMap, get_test_datums
from tests.conftest import collect_responses, drain_responses, send_test_requests


@pytest.fixture()
def map_test_server():
    class_instance = ExampleMap()
    my_server = MapServer(mapper_instance=class_instance)
    services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: my_server.servicer}
    return server_from_dictionary(services, strict_real_time())


def _invoke_map_fn(test_server, timeout=1):
    """Helper to invoke the MapFn stream method."""
    return test_server.invoke_stream_stream(
        method_descriptor=(map_pb2.DESCRIPTOR.services_by_name["Map"].methods_by_name["MapFn"]),
        invocation_metadata={},
        timeout=timeout,
    )


def test_init_with_args():
    my_servicer = MapServer(
        mapper_instance=map_handler,
        sock_path="/tmp/test.sock",
        max_message_size=1024 * 1024 * 5,
    )
    assert my_servicer.sock_path == "unix:///tmp/test.sock"
    assert my_servicer.max_message_size == 1024 * 1024 * 5


def test_udf_map_err_handshake():
    my_server = MapServer(mapper_instance=err_map_handler)
    services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: my_server.servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    test_datums = get_test_datums(handshake=False)
    method = _invoke_map_fn(test_server)
    send_test_requests(method, test_datums)
    drain_responses(method)

    metadata, code, details = method.termination()
    assert "MapFn: expected handshake as the first message" in details
    assert code == StatusCode.INTERNAL


def test_udf_map_error_response():
    my_server = MapServer(mapper_instance=err_map_handler)
    services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: my_server.servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    test_datums = get_test_datums(handshake=True)
    method = _invoke_map_fn(test_server)
    send_test_requests(method, test_datums)
    drain_responses(method)

    metadata, code, details = method.termination()
    assert "Something is fishy!" in details
    assert code == StatusCode.INTERNAL


def test_is_ready(map_test_server):
    method = map_test_server.invoke_unary_unary(
        method_descriptor=(map_pb2.DESCRIPTOR.services_by_name["Map"].methods_by_name["IsReady"]),
        invocation_metadata={},
        request=_empty_pb2.Empty(),
        timeout=1,
    )

    response, metadata, code, details = method.termination()
    assert response == map_pb2.ReadyResponse(ready=True)
    assert code == StatusCode.OK


def test_map_forward_message(map_test_server):
    test_datums = get_test_datums(handshake=True)
    method = _invoke_map_fn(map_test_server)
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
            "payload:test_mock_message "
            "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
            encoding="utf-8",
        )
        assert len(responses[idx].results) == 1
        idx += 1
    assert len(result_ids) == 0
    assert code == StatusCode.OK


def test_invalid_input():
    with pytest.raises(TypeError):
        MapServer()


def test_max_threads():
    # max cap at 16
    server = MapServer(mapper_instance=map_handler, max_threads=32)
    assert server.max_threads == 16

    # use argument provided
    server = MapServer(mapper_instance=map_handler, max_threads=5)
    assert server.max_threads == 5

    # defaults to 4
    server = MapServer(mapper_instance=map_handler)
    assert server.max_threads == 4
