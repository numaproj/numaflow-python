import unittest

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.sourcer import Sourcer
from pynumaflow.sourcer.proto import source_pb2
from tests.source.utils import (
    read_req_source_fn,
    ack_req_source_fn,
    err_sync_source_read_handler,
    err_sync_source_ack_handler,
    err_sync_source_pending_handler,
    err_sync_source_partition_handler,
)


class TestSyncSourcer(unittest.TestCase):
    def setUp(self) -> None:
        my_servicer = Sourcer(
            read_handler=err_sync_source_read_handler,
            ack_handler=err_sync_source_ack_handler,
            pending_handler=err_sync_source_pending_handler,
            partitions_handler=err_sync_source_partition_handler,
        )
        services = {source_pb2.DESCRIPTOR.services_by_name["Source"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_source_read_message(self):
        request = read_req_source_fn()

        method = self.test_server.invoke_unary_stream(
            method_descriptor=(
                source_pb2.DESCRIPTOR.services_by_name["Source"].methods_by_name["ReadFn"]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=source_pb2.ReadRequest(request=request),
            timeout=1,
        )

        metadata, code, details = method.termination()
        counter = 0
        # capture the output from the ReadFn generator and assert.
        while True:
            try:
                method.take_response()
                counter += 1
            except ValueError:
                break
        self.assertEqual(grpc.StatusCode.UNKNOWN, code)

    def test_source_ack(self):
        request = source_pb2.AckRequest(request=ack_req_source_fn())

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                source_pb2.DESCRIPTOR.services_by_name["Source"].methods_by_name["AckFn"]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=request,
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        self.assertEqual(grpc.StatusCode.UNKNOWN, code)

    def test_source_pending(self):
        request = _empty_pb2.Empty()

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                source_pb2.DESCRIPTOR.services_by_name["Source"].methods_by_name["PendingFn"]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=request,
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        self.assertEqual(grpc.StatusCode.UNKNOWN, code)

    def test_source_partition(self):
        request = _empty_pb2.Empty()

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                source_pb2.DESCRIPTOR.services_by_name["Source"].methods_by_name["PartitionsFn"]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=request,
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        self.assertEqual(grpc.StatusCode.UNKNOWN, code)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            Sourcer()


if __name__ == "__main__":
    unittest.main()
