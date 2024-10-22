import unittest
from unittest.mock import patch

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.proto.sourcetransformer import transform_pb2
from pynumaflow.sourcetransformer import SourceTransformServer
from tests.sourcetransform.utils import transform_handler, err_transform_handler
from tests.testing_utils import (
    mock_event_time,
    mock_watermark,
    mock_message,
    mock_terminate_on_stop,
    mock_new_event_time,
)


# We are mocking the terminate function from the psutil to not exit the program during testing
@patch("psutil.Process.kill", mock_terminate_on_stop)
class TestServer(unittest.TestCase):
    def setUp(self) -> None:
        server = SourceTransformServer(source_transform_instance=transform_handler)
        my_servicer = server.servicer
        services = {transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_init_with_args(self) -> None:
        server = SourceTransformServer(
            source_transform_instance=transform_handler,
            sock_path="/tmp/test.sock",
            max_message_size=1024 * 1024 * 5,
        )
        self.assertEqual(server.sock_path, "unix:///tmp/test.sock")
        self.assertEqual(server.max_message_size, 1024 * 1024 * 5)

    def test_udf_mapt_err(self):
        server = SourceTransformServer(source_transform_instance=err_transform_handler)
        my_servicer = server.servicer
        services = {transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        test_datums = [
            transform_pb2.SourceTransformRequest(
                handshake=transform_pb2.Handshake(sot=True),
            ),
            transform_pb2.SourceTransformRequest(
                request=transform_pb2.SourceTransformRequest.Request(
                    keys=["test"],
                    value=mock_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                    id="test-id-0",
                )
            ),
            transform_pb2.SourceTransformRequest(
                request=transform_pb2.SourceTransformRequest.Request(
                    keys=["test"],
                    value=mock_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                    id="test-id-1",
                )
            ),
            transform_pb2.SourceTransformRequest(
                request=transform_pb2.SourceTransformRequest.Request(
                    keys=["test"],
                    value=mock_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                    id="test-id-2",
                )
            ),
        ]

        method = self.test_server.invoke_stream_stream(
            method_descriptor=(
                transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"].methods_by_name[
                    "SourceTransformFn"
                ]
            ),
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
        self.assertTrue("Something is fishy" in details)
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

    def test_mapt_assign_new_event_time(self):
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        test_datums = [
            transform_pb2.SourceTransformRequest(
                handshake=transform_pb2.Handshake(sot=True),
            ),
            transform_pb2.SourceTransformRequest(
                request=transform_pb2.SourceTransformRequest.Request(
                    keys=["test"],
                    value=mock_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                    id="test-id-0",
                )
            ),
            transform_pb2.SourceTransformRequest(
                request=transform_pb2.SourceTransformRequest.Request(
                    keys=["test"],
                    value=mock_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                    id="test-id-1",
                )
            ),
            transform_pb2.SourceTransformRequest(
                request=transform_pb2.SourceTransformRequest.Request(
                    keys=["test"],
                    value=mock_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                    id="test-id-2",
                )
            ),
        ]

        method = self.test_server.invoke_stream_stream(
            method_descriptor=(
                transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"].methods_by_name[
                    "SourceTransformFn"
                ]
            ),
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
        # self.assertEqual(["test"], responses.results[0].keys)

        self.assertTrue(responses[0].handshake.sot)

        self.assertEqual("test-id-0", responses[1].id)
        self.assertEqual(
            bytes(
                "payload:test_mock_message " "event_time:2022-09-12 16:00:00 ",
                encoding="utf-8",
            ),
            responses[1].results[0].value,
        )
        self.assertEqual(1, len(responses[1].results))

        self.assertEqual("test-id-1", responses[2].id)
        self.assertEqual(
            bytes(
                "payload:test_mock_message " "event_time:2022-09-12 16:00:00 ",
                encoding="utf-8",
            ),
            responses[2].results[0].value,
        )
        self.assertEqual(1, len(responses[2].results))

        self.assertEqual("test-id-2", responses[3].id)
        self.assertEqual(
            bytes(
                "payload:test_mock_message " "event_time:2022-09-12 16:00:00 ",
                encoding="utf-8",
            ),
            responses[3].results[0].value,
        )
        self.assertEqual(1, len(responses[3].results))
        # Verify new event time gets assigned.
        updated_event_time_timestamp = _timestamp_pb2.Timestamp()
        updated_event_time_timestamp.FromDatetime(dt=mock_new_event_time())
        self.assertEqual(
            updated_event_time_timestamp,
            responses[1].results[0].event_time,
        )
        self.assertEqual(code, StatusCode.OK)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            SourceTransformServer()

    def test_max_threads(self):
        # max cap at 16
        server = SourceTransformServer(source_transform_instance=transform_handler, max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = SourceTransformServer(source_transform_instance=transform_handler, max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = SourceTransformServer(source_transform_instance=transform_handler)
        self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    unittest.main()
