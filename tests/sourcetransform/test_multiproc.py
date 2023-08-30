import os
import unittest
from unittest import mock

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.sourcetransform.multiproc_server import MultiProcSourceTransformer
from pynumaflow.sourcetransform.proto import transform_pb2_grpc, transform_pb2
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
        my_servicer = MultiProcSourceTransformer(
            transform_handler=transform_handler,
        )
        services = {transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    @mockenv(NUM_CPU_MULTIPROC="3")
    def test_multiproc_init(self) -> None:
        server = MultiProcSourceTransformer(
            transform_handler=transform_handler,
        )
        self.assertEqual(server._sock_path, 55551)
        self.assertEqual(server._process_count, 3)

    @mockenv(NUMAFLOW_CPU_LIMIT="4")
    def test_multiproc_process_count(self) -> None:
        server = MultiProcSourceTransformer(
            transform_handler=transform_handler,
        )
        self.assertEqual(server._sock_path, 55551)
        self.assertEqual(server._process_count, 4)

    # To test the reuse property for the grpc servers which allow multiple
    # bindings to the same server
    def test_reuse_port(self):
        serv_options = [("grpc.so_reuseport", 1), ("grpc.so_reuseaddr", 1)]

        server = MultiProcSourceTransformer(
            transform_handler=transform_handler,
        )

        with server._reserve_port() as port:
            print(port)
            bind_address = f"localhost:{port}"
            server1 = grpc.server(thread_pool=None, options=serv_options)
            transform_pb2_grpc.add_SourceTransformServicer_to_server(server, server1)
            server1.add_insecure_port(bind_address)

            # so_reuseport=0 -> the bind should raise an error
            server2 = grpc.server(thread_pool=None, options=(("grpc.so_reuseport", 0),))
            transform_pb2_grpc.add_SourceTransformServicer_to_server(server, server2)
            self.assertRaises(RuntimeError, server2.add_insecure_port, bind_address)

            # so_reuseport=1 -> should allow server to bind to port again
            server3 = grpc.server(thread_pool=None, options=(("grpc.so_reuseport", 1),))
            transform_pb2_grpc.add_SourceTransformServicer_to_server(server, server3)
            server3.add_insecure_port(bind_address)

    def test_udf_mapt_err(self):
        my_servicer = MultiProcSourceTransformer(transform_handler=err_transform_handler)
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
        with self.assertRaises(ValueError):
            MultiProcSourceTransformer()


if __name__ == "__main__":
    unittest.main()
