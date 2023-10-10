import unittest

from google.protobuf import empty_pb2 as _empty_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.sourcer import Sourcer
from pynumaflow.sourcer.proto import source_pb2
from tests.source.utils import (
    sync_source_read_handler,
    sync_source_ack_handler,
    sync_source_pending_handler,
    read_req_source_fn,
    mock_offset,
    ack_req_source_fn,
)


class TestSyncSourcer(unittest.TestCase):
    def setUp(self) -> None:
        my_servicer = Sourcer(
            read_handler=sync_source_read_handler,
            ack_handler=sync_source_ack_handler,
            pending_handler=sync_source_pending_handler,
        )
        services = {source_pb2.DESCRIPTOR.services_by_name["Source"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_init_with_args(self) -> None:
        my_servicer = Sourcer(
            read_handler=sync_source_read_handler,
            ack_handler=sync_source_ack_handler,
            pending_handler=sync_source_pending_handler,
            sock_path="/tmp/test.sock",
            max_message_size=1024 * 1024 * 5,
        )
        self.assertEqual(my_servicer.sock_path, "unix:///tmp/test.sock")
        self.assertEqual(my_servicer._max_message_size, 1024 * 1024 * 5)

    def test_is_ready(self):
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                source_pb2.DESCRIPTOR.services_by_name["Source"].methods_by_name["IsReady"]
            ),
            invocation_metadata={},
            request=_empty_pb2.Empty(),
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        expected = source_pb2.ReadyResponse(ready=True)
        self.assertEqual(expected, response)
        self.assertEqual(code, StatusCode.OK)

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
        # r = method.take_response()
        # print(r)
        while True:
            try:
                r = method.take_response()
                counter += 1
            except ValueError:
                break

            self.assertEqual(
                bytes("payload:test_mock_message", encoding="utf-8"),
                r.result.payload,
            )
            self.assertEqual(
                ["test_key"],
                r.result.keys,
            )
            self.assertEqual(
                mock_offset().offset,
                r.result.offset.offset,
            )
            self.assertEqual(
                mock_offset().partition_id,
                r.result.offset.partition_id,
            )
        """Assert that the generator was called 10 times in the stream"""
        self.assertEqual(10, counter)
        self.assertEqual(code, StatusCode.OK)

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
        self.assertEqual(response, source_pb2.AckResponse())

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
        self.assertEqual(response.result.count, 10)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            Sourcer()


if __name__ == "__main__":
    unittest.main()
