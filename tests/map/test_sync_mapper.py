import unittest
from unittest.mock import patch

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.mapper import MapServer
from pynumaflow.proto.mapper import map_pb2
from tests.map.utils import map_handler, err_map_handler, ExampleMap, get_test_datums
from tests.testing_utils import (
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

    def test_udf_map_err_handshake(self):
        my_server = MapServer(mapper_instance=err_map_handler)
        services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: my_server.servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        test_datums = get_test_datums(handshake=False)
        method = self.test_server.invoke_stream_stream(
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
                if "No more responses!" in err.__str__():
                    break

        metadata, code, details = method.termination()
        self.assertTrue("MapFn: expected handshake as the first message" in details)
        self.assertEqual(grpc.StatusCode.UNKNOWN, code)

    def test_udf_map_error_response(self):
        my_server = MapServer(mapper_instance=err_map_handler)
        services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: my_server.servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        test_datums = get_test_datums(handshake=True)
        method = self.test_server.invoke_stream_stream(
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
                if "No more responses!" in err.__str__():
                    break

        metadata, code, details = method.termination()
        self.assertTrue("Something is fishy!" in details)
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
        test_datums = get_test_datums(handshake=True)
        method = self.test_server.invoke_stream_stream(
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
                if "No more responses!" in err.__str__():
                    break

        metadata, code, details = method.termination()
        # 1 handshake + 3 data responses
        self.assertEqual(4, len(responses))

        self.assertTrue(responses[0].handshake.sot)

        idx = 1
        while idx < len(responses):
            _id = "test-id-" + str(idx)
            self.assertEqual(_id, responses[idx].id)
            self.assertEqual(
                bytes(
                    "payload:test_mock_message "
                    "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                    encoding="utf-8",
                ),
                responses[idx].results[0].value,
            )
            self.assertEqual(1, len(responses[idx].results))
            idx += 1
        self.assertEqual(code, StatusCode.OK)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            MapServer()

    def test_max_threads(self):
        # max cap at 16
        server = MapServer(mapper_instance=map_handler, max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = MapServer(mapper_instance=map_handler, max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = MapServer(mapper_instance=map_handler)
        self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    unittest.main()
