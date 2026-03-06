from unittest.mock import MagicMock

import grpc
import pytest
from google.protobuf import empty_pb2 as _empty_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.mapper import MapServer
from pynumaflow.mapper._servicer._sync_servicer import SyncMapServicer
from pynumaflow.proto.mapper import map_pb2
from tests.map.utils import map_handler, err_map_handler, ExampleMap, get_test_datums


@pytest.fixture
def test_server():
    class_instance = ExampleMap()
    my_server = MapServer(mapper_instance=class_instance)
    services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: my_server.servicer}
    return server_from_dictionary(services, strict_real_time())


def _invoke_map(test_server, handshake=True):
    test_datums = get_test_datums(handshake=handshake)
    method = test_server.invoke_stream_stream(
        method_descriptor=(map_pb2.DESCRIPTOR.services_by_name["Map"].methods_by_name["MapFn"]),
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
    return method, responses


def _err_server():
    my_server = MapServer(mapper_instance=err_map_handler)
    services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: my_server.servicer}
    return server_from_dictionary(services, strict_real_time())


class TestSyncMapper:
    def test_init_with_args(self):
        my_servicer = MapServer(
            mapper_instance=map_handler,
            sock_path="/tmp/test.sock",
            max_message_size=1024 * 1024 * 5,
        )
        assert my_servicer.sock_path == "unix:///tmp/test.sock"
        assert my_servicer.max_message_size == 1024 * 1024 * 5

    def test_udf_map_err_handshake(self):
        server = _err_server()
        method, responses = _invoke_map(server, handshake=False)

        metadata, code, details = method.termination()
        assert "MapFn: expected handshake as the first message" in details
        assert code == grpc.StatusCode.INTERNAL

    def test_udf_map_error_response(self):
        server = _err_server()
        method, responses = _invoke_map(server, handshake=True)

        metadata, code, details = method.termination()
        assert "Something is fishy!" in details
        assert code == grpc.StatusCode.INTERNAL

    def test_is_ready(self, test_server):
        method = test_server.invoke_unary_unary(
            method_descriptor=(
                map_pb2.DESCRIPTOR.services_by_name["Map"].methods_by_name["IsReady"]
            ),
            invocation_metadata={},
            request=_empty_pb2.Empty(),
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        assert response == map_pb2.ReadyResponse(ready=True)
        assert code == StatusCode.OK

    def test_map_forward_message(self, test_server):
        method, responses = _invoke_map(test_server, handshake=True)

        metadata, code, details = method.termination()
        # 1 handshake + 3 data responses
        assert len(responses) == 4
        assert responses[0].handshake.sot

        result_ids = {f"test-id-{id}" for id in range(1, 4)}
        for resp in responses[1:]:
            result_ids.remove(resp.id)
            assert resp.results[0].value == bytes(
                "payload:test_mock_message "
                "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                encoding="utf-8",
            )
            assert len(resp.results) == 1
        assert len(result_ids) == 0
        assert code == StatusCode.OK

    def test_invalid_input(self):
        with pytest.raises(TypeError):
            MapServer()

    def test_max_threads(self):
        # max cap at 16
        server = MapServer(mapper_instance=map_handler, max_threads=32)
        assert server.max_threads == 16

        # use argument provided
        server = MapServer(mapper_instance=map_handler, max_threads=5)
        assert server.max_threads == 5

        # defaults to 4
        server = MapServer(mapper_instance=map_handler)
        assert server.max_threads == 4

    def test_rpc_error_before_handshake(self):
        """RpcError on the very first read triggers the except grpc.RpcError in MapFn."""

        def failing_iterator():
            raise grpc.RpcError()
            yield  # make it a generator

        servicer = SyncMapServicer(handler=map_handler)
        context = MagicMock()

        responses = list(servicer.MapFn(failing_iterator(), context))

        assert responses == []
        assert servicer.shutdown_event.is_set()

    def test_rpc_error_mid_stream(self):
        """RpcError while reading requests triggers the except grpc.RpcError
        in _process_requests."""

        def interrupted_iterator():
            yield map_pb2.MapRequest(handshake=map_pb2.Handshake(sot=True))
            raise grpc.RpcError()

        servicer = SyncMapServicer(handler=map_handler)
        context = MagicMock()

        responses = list(servicer.MapFn(interrupted_iterator(), context))

        # Should get the handshake response and then stop cleanly
        assert len(responses) == 1
        assert responses[0].handshake.sot
        assert servicer.shutdown_event.is_set()
