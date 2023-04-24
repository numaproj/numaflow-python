import os
import unittest
from unittest import mock

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.function.multiproc_server import MultiProcServer
from pynumaflow.function.proto import udfunction_pb2_grpc, udfunction_pb2
from pynumaflow.tests.function.testing_utils import (
    mapt_handler,
    map_handler,
    err_map_handler,
    mock_event_time,
    mock_watermark,
    mock_message,
    mock_new_event_time,
    err_mapt_handler,
)
from pynumaflow.tests.function.test_async_server import async_reduce_handler


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


class TestMultiProcMethods(unittest.TestCase):
    def setUp(self) -> None:
        my_servicer = MultiProcServer(
            map_handler=map_handler, mapt_handler=mapt_handler, reduce_handler=async_reduce_handler
        )
        services = {udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    @mockenv(NUM_CPU_MULTIPROC="3")
    def test_multiproc_init(self) -> None:
        server = MultiProcServer(
            reduce_handler=async_reduce_handler, map_handler=map_handler, mapt_handler=mapt_handler
        )
        self.assertEqual(server._sock_path, 55551)
        self.assertEqual(server._process_count, 3)

    @mockenv(NUMAFLOW_CPU_LIMIT="4")
    def test_multiproc_process_count(self) -> None:
        server = MultiProcServer(
            reduce_handler=async_reduce_handler, map_handler=map_handler, mapt_handler=mapt_handler
        )
        self.assertEqual(server._sock_path, 55551)
        self.assertEqual(server._process_count, 4)

    # To test the reuse property for the grpc servers which allow multiple
    # bindings to the same server
    def test_reuse_port(self):
        serv_options = [("grpc.so_reuseport", 1), ("grpc.so_reuseaddr", 1)]

        server = MultiProcServer(
            reduce_handler=async_reduce_handler, map_handler=map_handler, mapt_handler=mapt_handler
        )

        with server._reserve_port() as port:
            print(port)
            bind_address = f"localhost:{port}"
            server1 = grpc.server(thread_pool=None, options=serv_options)
            udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(server, server1)
            server1.add_insecure_port(bind_address)

            # so_reuseport=0 -> the bind should raise an error
            server2 = grpc.server(thread_pool=None, options=(("grpc.so_reuseport", 0),))
            udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(server, server2)
            self.assertRaises(RuntimeError, server2.add_insecure_port, bind_address)

            # so_reuseport=1 -> should allow server to bind to port again
            server3 = grpc.server(thread_pool=None, options=(("grpc.so_reuseport", 1),))
            udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(server, server3)
            server3.add_insecure_port(bind_address)

    def test_udf_map_err(self):
        my_servicer = MultiProcServer(map_handler=err_map_handler)
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
        my_servicer = MultiProcServer(mapt_handler=err_mapt_handler)
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
            MultiProcServer()

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


if __name__ == "__main__":
    unittest.main()
