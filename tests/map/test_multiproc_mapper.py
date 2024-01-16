import os
import unittest
from unittest import mock

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.mapper import MapMultiprocServer
from pynumaflow.proto.mapper import map_pb2
from tests.map.utils import map_handler, err_map_handler
from tests.testing_utils import (
    mock_event_time,
    mock_watermark,
    mock_message,
)


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


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

    def test_udf_map_err(self):
        my_server = MapMultiprocServer(mapper_instance=err_map_handler)
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
            MapMultiprocServer()


if __name__ == "__main__":
    unittest.main()
