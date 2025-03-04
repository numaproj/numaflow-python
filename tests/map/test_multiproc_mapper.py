import os
import unittest
from unittest.mock import patch

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.mapper import MapMultiprocServer
from pynumaflow.proto.mapper import map_pb2
from tests.map.utils import map_handler, err_map_handler, get_test_datums
from tests.testing_utils import (
    mock_terminate_on_stop,
)


# We are mocking the terminate function from the psutil to not exit the program during testing
@patch("psutil.Process.kill", mock_terminate_on_stop)
class TestMultiProcMethods(unittest.TestCase):
    def setUp(self) -> None:
        my_server = MapMultiprocServer(mapper_instance=map_handler)
        services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: my_server.servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_multiproc_init(self) -> None:
        my_server = MapMultiprocServer(mapper_instance=map_handler, server_count=3)
        self.assertEqual(my_server._process_count, 3)

    def test_multiproc_process_count(self) -> None:
        default_val = os.cpu_count()
        my_server = MapMultiprocServer(mapper_instance=map_handler)
        self.assertEqual(my_server._process_count, default_val)

    def test_max_process_count(self) -> None:
        """Max process count is capped at 2 * os.cpu_count, irrespective of what the user
        provides as input"""
        default_val = os.cpu_count()
        server = MapMultiprocServer(mapper_instance=map_handler, server_count=20)
        self.assertEqual(server._process_count, default_val * 2)

    def test_udf_map_err_handshake(self):
        my_server = MapMultiprocServer(mapper_instance=err_map_handler)
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
        self.assertEqual(grpc.StatusCode.INTERNAL, code)

    def test_udf_map_err(self):
        my_server = MapMultiprocServer(mapper_instance=err_map_handler)
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
        self.assertEqual(grpc.StatusCode.INTERNAL, code)

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
            MapMultiprocServer()


if __name__ == "__main__":
    unittest.main()
