import unittest

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time
from pynumaflow.sideinput import SideInput
from pynumaflow.sideinput.proto import sideinput_pb2

from pynumaflow.sideinput import Response


def retrieve_side_input_handler() -> Response:
    msg = mock_message()
    return Response.broadcast_message(msg)


def retrieve_no_broadcast_handler() -> Response:
    return Response.no_broadcast_message()


def err_retrieve_handler() -> Response:
    raise RuntimeError("Something is fishy!")


def mock_message():
    msg = bytes("test_side_input", encoding="utf-8")
    return msg


class TestServer(unittest.TestCase):
    def setUp(self) -> None:
        my_service = SideInput(handler=retrieve_side_input_handler)
        services = {sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"]: my_service}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_init_with_args(self) -> None:
        my_servicer = SideInput(
            handler=retrieve_side_input_handler,
            sock_path="/tmp/test_side_input.sock",
            max_message_size=1024 * 1024 * 5,
        )
        self.assertEqual(my_servicer.sock_path, "unix:///tmp/test_side_input.sock")
        self.assertEqual(my_servicer._max_message_size, 1024 * 1024 * 5)

    def test_side_input_err(self):
        my_servicer = SideInput(handler=err_retrieve_handler)
        services = {sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"].methods_by_name[
                    "RetrieveSideInput"
                ]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=_empty_pb2.Empty(),
            timeout=1,
        )
        response, metadata, code, details = method.termination()
        self.assertEqual(grpc.StatusCode.UNKNOWN, code)

    def test_is_ready(self):
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"].methods_by_name["IsReady"]
            ),
            invocation_metadata={},
            request=_empty_pb2.Empty(),
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        expected = sideinput_pb2.ReadyResponse(ready=True)
        self.assertEqual(expected, response)
        self.assertEqual(code, StatusCode.OK)

    def test_side_input_message(self):
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"].methods_by_name[
                    "RetrieveSideInput"
                ]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=_empty_pb2.Empty(),
            timeout=1,
        )
        response, metadata, code, details = method.termination()
        self.assertEqual(mock_message(), response.value)
        self.assertEqual(code, StatusCode.OK)

    def test_side_input_no_broadcast(self):
        my_servicer = SideInput(handler=retrieve_no_broadcast_handler)
        services = {sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"].methods_by_name[
                    "RetrieveSideInput"
                ]
            ),
            invocation_metadata={
                ("this_metadata_will_be_skipped", "test_ignore"),
            },
            request=_empty_pb2.Empty(),
            timeout=1,
        )
        response, metadata, code, details = method.termination()
        self.assertEqual(code, StatusCode.OK)
        self.assertEqual(response.no_broadcast, True)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            SideInput()


if __name__ == "__main__":
    unittest.main()
