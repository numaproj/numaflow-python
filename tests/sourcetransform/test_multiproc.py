import os
import unittest
from unittest import mock
from unittest.mock import Mock, patch

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time
from pynumaflow._constants import ServerType

from pynumaflow.proto.sourcetransformer import transform_pb2
from pynumaflow.sourcetransformer import SourceTransformServer
from tests.sourcetransform.utils import transform_handler, err_transform_handler
from tests.testing_utils import (
    mock_event_time,
    mock_watermark,
    mock_message,
    mock_new_event_time,
    get_time_args,
)


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


class TestMultiProcMethods(unittest.TestCase):
    def setUp(self) -> None:
        server = SourceTransformServer(
            source_transform_instance=transform_handler, server_type=ServerType.Multiproc
        )
        my_servicer = server.get_servicer(
            source_transform_instance=server.source_transform_instance,
            server_type=server.server_type,
        )
        services = {transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    @mockenv(NUM_CPU_MULTIPROC="3")
    def test_multiproc_init(self) -> None:
        server = SourceTransformServer(
            source_transform_instance=transform_handler, server_type=ServerType.Multiproc
        )
        self.assertEqual(server._process_count, 3)

    @patch("os.cpu_count", Mock(return_value=4))
    def test_multiproc_process_count(self) -> None:
        server = SourceTransformServer(
            source_transform_instance=transform_handler, server_type=ServerType.Multiproc
        )
        self.assertEqual(server._process_count, 4)

    @patch("os.cpu_count", Mock(return_value=4))
    @mockenv(NUM_CPU_MULTIPROC="10")
    def test_max_process_count(self) -> None:
        server = SourceTransformServer(
            source_transform_instance=transform_handler, server_type=ServerType.Multiproc
        )
        self.assertEqual(server._process_count, 8)

    def test_udf_mapt_err(self):
        server = SourceTransformServer(
            source_transform_instance=err_transform_handler, server_type=ServerType.Multiproc
        )
        my_servicer = server.get_servicer(
            source_transform_instance=server.source_transform_instance,
            server_type=server.server_type,
        )
        services = {transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = transform_pb2.SourceTransformRequest(
            value=mock_message(),
            event_time=event_time_timestamp,
            watermark=watermark_timestamp,
        )

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"].methods_by_name[
                    "SourceTransformFn"
                ]
            ),
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
                transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"].methods_by_name[
                    "IsReady"
                ]
            ),
            invocation_metadata={},
            request=_empty_pb2.Empty(),
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        expected = transform_pb2.ReadyResponse(ready=True)
        self.assertEqual(expected, response)
        self.assertEqual(code, StatusCode.OK)

    def test_mapt_assign_new_event_time(self, test_server=None):
        event_time_timestamp, watermark_timestamp = get_time_args()

        request = transform_pb2.SourceTransformRequest(
            keys=["test"],
            value=mock_message(),
            event_time=event_time_timestamp,
            watermark=watermark_timestamp,
        )
        serv = self.test_server
        if test_server:
            serv = test_server

        method = serv.invoke_unary_unary(
            method_descriptor=(
                transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"].methods_by_name[
                    "SourceTransformFn"
                ]
            ),
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
                "payload:test_mock_message " "event_time:2022-09-12 16:00:00 ",
                encoding="utf-8",
            ),
            response.results[0].value,
        )
        # Verify new event time gets assigned.
        updated_event_time_timestamp = _timestamp_pb2.Timestamp()
        updated_event_time_timestamp.FromDatetime(dt=mock_new_event_time())
        self.assertEqual(
            updated_event_time_timestamp,
            response.results[0].event_time,
        )
        self.assertEqual(code, StatusCode.OK)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            SourceTransformServer(server_type=ServerType.Multiproc)


if __name__ == "__main__":
    unittest.main()
