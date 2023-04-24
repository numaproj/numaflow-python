import unittest

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.function import Server
from pynumaflow.function.proto import udfunction_pb2
from pynumaflow.tests.function.testing_utils import (
    mapt_handler,
    map_handler,
    reduce_handler,
    err_map_handler,
    mock_event_time,
    mock_watermark,
    mock_message,
    err_mapt_handler,
    mock_new_event_time,
)


class TestServer(unittest.TestCase):
    def setUp(self) -> None:
        my_servicer = Server(
            map_handler=map_handler, mapt_handler=mapt_handler, reduce_handler=reduce_handler
        )
        services = {udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_init_with_args(self) -> None:
        my_servicer = Server(
            map_handler=map_handler,
            mapt_handler=mapt_handler,
            reduce_handler=reduce_handler,
            sock_path="/tmp/test.sock",
            max_message_size=1024 * 1024 * 5,
        )
        self.assertEqual(my_servicer.sock_path, "unix:///tmp/test.sock")
        self.assertEqual(my_servicer._max_message_size, 1024 * 1024 * 5)

    def test_udf_map_err(self):
        my_servicer = Server(map_handler=err_map_handler)
        services = {udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.DatumRequest(
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"].methods_by_name[
                    "MapFn"
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

    def test_udf_map_error_response(self):
        my_servicer = Server(map_handler=err_map_handler)
        services = {udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.DatumRequest(
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"].methods_by_name[
                    "MapFn"
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

    def test_udf_mapt_err(self):
        my_servicer = Server(mapt_handler=err_mapt_handler)
        services = {udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.DatumRequest(
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"].methods_by_name[
                    "MapTFn"
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

    def test_unimplemented_reduce(self):
        method = self.test_server.invoke_stream_stream(
            method_descriptor=(
                udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"].methods_by_name[
                    "ReduceFn"
                ]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            timeout=1,
        )

        metadata, code, details = method.termination()

        self.assertEqual(grpc.StatusCode.UNIMPLEMENTED, code)
        self.assertEqual("Method not implemented!", details)

    def test_is_ready(self):
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"].methods_by_name[
                    "IsReady"
                ]
            ),
            invocation_metadata={},
            request=_empty_pb2.Empty(),
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        expected = udfunction_pb2.ReadyResponse(ready=True)
        self.assertEqual(expected, response)
        self.assertEqual(code, StatusCode.OK)

    def test_map_forward_message(self):
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.DatumRequest(
            keys=["test"],
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"].methods_by_name[
                    "MapFn"
                ]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=request,
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        self.assertEqual(1, len(response.elements))
        self.assertEqual(["test"], response.elements[0].keys)
        self.assertEqual(
            bytes(
                "payload:test_mock_message "
                "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                encoding="utf-8",
            ),
            response.elements[0].value,
        )
        self.assertEqual(code, StatusCode.OK)

    def test_mapt_assign_new_event_time(self, test_server=None):
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.DatumRequest(
            keys=["test"],
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )
        serv = self.test_server
        if test_server:
            serv = test_server

        method = serv.invoke_unary_unary(
            method_descriptor=(
                udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"].methods_by_name[
                    "MapTFn"
                ]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=request,
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        self.assertEqual(1, len(response.elements))
        self.assertEqual(["test"], response.elements[0].keys)
        self.assertEqual(
            bytes(
                "payload:test_mock_message "
                "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                encoding="utf-8",
            ),
            response.elements[0].value,
        )
        # Verify new event time gets assigned.
        updated_event_time_timestamp = _timestamp_pb2.Timestamp()
        updated_event_time_timestamp.FromDatetime(dt=mock_new_event_time())
        self.assertEqual(
            udfunction_pb2.EventTime(event_time=updated_event_time_timestamp),
            response.elements[0].event_time,
        )
        self.assertEqual(code, StatusCode.OK)

    def test_invalid_input(self):
        with self.assertRaises(ValueError):
            Server()


if __name__ == "__main__":
    unittest.main()
