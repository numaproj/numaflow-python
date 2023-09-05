import unittest

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.sourcetransformer import SourceTransformer
from pynumaflow.sourcetransformer.proto import transform_pb2
from tests.sourcetransform.utils import transform_handler, err_transform_handler
from tests.testing_utils import (
    mock_event_time,
    mock_watermark,
    mock_message,
    mock_new_event_time,
)


class TestServer(unittest.TestCase):
    def setUp(self) -> None:
        my_servicer = SourceTransformer(handler=transform_handler)
        services = {transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_init_with_args(self) -> None:
        my_servicer = SourceTransformer(
            handler=transform_handler, sock_path="/tmp/test.sock", max_message_size=1024 * 1024 * 5
        )
        self.assertEqual(my_servicer.sock_path, "unix:///tmp/test.sock")
        self.assertEqual(my_servicer._max_message_size, 1024 * 1024 * 5)

    def test_udf_mapt_err(self):
        my_servicer = SourceTransformer(handler=err_transform_handler)
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
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

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
            SourceTransformer()


if __name__ == "__main__":
    unittest.main()
