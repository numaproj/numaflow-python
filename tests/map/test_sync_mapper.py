import unittest
from unittest.mock import patch

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.mapper import MapServer
from pynumaflow.proto.mapper import map_pb2
from tests.map.utils import map_handler, err_map_handler, ExampleMap
from tests.testing_utils import (
    mock_event_time,
    mock_watermark,
    mock_message,
    mock_headers,
    mock_terminate_on_stop,
)


# We are mocking the terminate function from the psutil to not exit the program during testing
@patch("psutil.Process.kill", mock_terminate_on_stop)
class TestSyncMapper(unittest.TestCase):
    # @patch("psutil.Process.kill", mock_terminate_on_stop)
    def setUp(self) -> None:
        class_instance = ExampleMap()
        my_server = MapServer(mapper_instance=class_instance)
        services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: my_server.servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_init_with_args(self) -> None:
        my_servicer = MapServer(
            mapper_instance=map_handler,
            sock_path="/tmp/test.sock",
            max_message_size=1024 * 1024 * 5,
        )
        self.assertEqual(my_servicer.sock_path, "unix:///tmp/test.sock")
        self.assertEqual(my_servicer.max_message_size, 1024 * 1024 * 5)

    def test_udf_map_err(self):
        my_server = MapServer(mapper_instance=err_map_handler)
        services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: my_server.servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = map_pb2.MapRequest(
            value=mock_message(),
            event_time=event_time_timestamp,
            watermark=watermark_timestamp,
            headers=mock_headers(),
        )

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(map_pb2.DESCRIPTOR.services_by_name["Map"].methods_by_name["MapFn"]),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=request,
            timeout=1,
        )
        response, metadata, code, details = method.termination()
        self.assertEqual(grpc.StatusCode.UNKNOWN, code)

    def test_udf_map_error_response(self):
        my_server = MapServer(mapper_instance=err_map_handler)
        services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: my_server.servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = map_pb2.MapRequest(
            value=mock_message(),
            event_time=event_time_timestamp,
            watermark=watermark_timestamp,
            headers=mock_headers(),
        )

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(map_pb2.DESCRIPTOR.services_by_name["Map"].methods_by_name["MapFn"]),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=request,
            timeout=1,
        )
        response, metadata, code, details = method.termination()
        self.assertEqual(grpc.StatusCode.UNKNOWN, code)

    def test_is_ready(self):
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                map_pb2.DESCRIPTOR.services_by_name["Map"].methods_by_name["IsReady"]
            ),
            invocation_metadata={},
            request=_empty_pb2.Empty(),
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        expected = map_pb2.ReadyResponse(ready=True)
        self.assertEqual(expected, response)
        self.assertEqual(code, StatusCode.OK)

    def test_map_forward_message(self):
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = map_pb2.MapRequest(
            keys=["test"],
            value=mock_message(),
            event_time=event_time_timestamp,
            watermark=watermark_timestamp,
            headers=mock_headers(),
        )

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(map_pb2.DESCRIPTOR.services_by_name["Map"].methods_by_name["MapFn"]),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=request,
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        self.assertEqual(1, len(response.results))
        self.assertEqual(["test"], response.results[0].keys)
        self.assertEqual(
            bytes(
                "payload:test_mock_message "
                "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                encoding="utf-8",
            ),
            response.results[0].value,
        )
        self.assertEqual(code, StatusCode.OK)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            MapServer()


if __name__ == "__main__":
    unittest.main()
