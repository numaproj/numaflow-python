import unittest
from unittest.mock import patch

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.proto.common import metadata_pb2
from pynumaflow.proto.sourcetransformer import transform_pb2
from pynumaflow.sourcetransformer import SourceTransformServer, Datum, Messages, Message
from tests.sourcetransform.utils import transform_handler, err_transform_handler, get_test_datums
from tests.testing_utils import (
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

        test_datums = get_test_datums()

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
        self.assertEqual(grpc.StatusCode.INTERNAL, code)

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

    def test_udf_mapt_err_handshake(self):
        server = SourceTransformServer(source_transform_instance=err_transform_handler)
        my_servicer = server.servicer
        services = {transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        test_datums = get_test_datums(handshake=False)
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
        self.assertTrue("SourceTransformFn: expected handshake message" in details)
        self.assertEqual(grpc.StatusCode.INTERNAL, code)

    def test_mapt_assign_new_event_time(self):
        test_datums = get_test_datums()

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

        self.assertTrue(responses[0].handshake.sot)

        result_ids = {f"test-id-{id}" for id in range(1, 4)}
        idx = 1
        while idx < len(responses):
            result_ids.remove(responses[idx].id)
            self.assertEqual(
                bytes(
                    "payload:test_mock_message " "event_time:2022-09-12 16:00:00 ",
                    encoding="utf-8",
                ),
                responses[idx].results[0].value,
            )
            self.assertEqual(1, len(responses[idx].results))
            idx += 1
        self.assertEqual(len(result_ids), 0)

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


def metadata_transform_handler(keys: list[str], datum: Datum) -> Messages:
    """Handler that validates system metadata and passes through user metadata."""
    if datum.system_metadata.value("numaflow_version_info", "version") != b"1.0.0":
        raise ValueError("System metadata version mismatch")

    val = datum.value
    msg = "payload:{} event_time:{} ".format(
        val.decode("utf-8"),
        datum.event_time,
    )
    val = bytes(msg, encoding="utf-8")
    messages = Messages()
    messages.append(
        Message(val, mock_new_event_time(), keys=keys, user_metadata=datum.user_metadata)
    )
    return messages


@patch("psutil.Process.kill", mock_terminate_on_stop)
class TestServerMetadata(unittest.TestCase):
    def setUp(self) -> None:
        server = SourceTransformServer(source_transform_instance=metadata_transform_handler)
        my_servicer = server.servicer
        services = {transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_source_transform_with_metadata(self):
        test_datums = get_test_datums(with_metadata=True)

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
        self.assertTrue(responses[0].handshake.sot)

        # Verify metadata is passed through correctly
        result_metadata = {}
        for resp in responses[1:]:
            result_metadata[resp.id] = resp.results[0].metadata

        for idx in range(1, 4):
            _id = f"test-id-{idx}"
            self.assertIn(_id, result_metadata)
            self.assertEqual(
                result_metadata[_id].user_metadata["custom_info"],
                metadata_pb2.KeyValueGroup(key_value={"version": f"{idx}.0.0".encode()}),
            )
            # System metadata should be empty in responses
            self.assertEqual(result_metadata[_id].sys_metadata, {})

        self.assertEqual(code, StatusCode.OK)


if __name__ == "__main__":
    unittest.main()
