import os
import unittest
from collections.abc import Iterator
from datetime import datetime, timezone
from unittest import mock
from unittest.mock import patch

from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow._constants import (
    UD_CONTAINER_FALLBACK_SINK,
    FALLBACK_SINK_SOCK_PATH,
    FALLBACK_SINK_SERVER_INFO_FILE_PATH,
)
from pynumaflow.proto.sinker import sink_pb2
from pynumaflow.sinker import Responses, Datum, Response, SinkServer
from tests.testing_utils import mock_terminate_on_stop


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


def udsink_handler(datums: Iterator[Datum]) -> Responses:
    results = Responses()
    for msg in datums:
        if "err" in msg.value.decode("utf-8"):
            results.append(Response.as_failure(msg.id, "mock sink message error"))
        elif "fallback" in msg.value.decode("utf-8"):
            results.append(Response.as_fallback(msg.id))
        else:
            results.append(Response.as_success(msg.id))
    return results


def err_udsink_handler(_: Iterator[Datum]) -> Responses:
    raise RuntimeError("Something is fishy!")


def mock_message():
    msg = bytes("test_mock_message", encoding="utf-8")
    return msg


def mock_err_message():
    msg = bytes("test_mock_err_message", encoding="utf-8")
    return msg


def mock_fallback_message():
    msg = bytes("test_mock_fallback_message", encoding="utf-8")
    return msg


def mock_event_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


def mock_watermark():
    t = datetime.fromtimestamp(1662998460, timezone.utc)
    return t


# We are mocking the terminate function from the psutil to not exit the program during testing
@patch("psutil.Process.kill", mock_terminate_on_stop)
class TestServer(unittest.TestCase):
    def setUp(self) -> None:
        server = SinkServer(sinker_instance=udsink_handler)
        my_servicer = server.servicer
        services = {sink_pb2.DESCRIPTOR.services_by_name["Sink"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    def test_is_ready(self):
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["IsReady"]
            ),
            invocation_metadata={},
            request=_empty_pb2.Empty(),
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        expected = sink_pb2.ReadyResponse(ready=True)
        self.assertEqual(expected, response)
        self.assertEqual(code, StatusCode.OK)

    def test_udsink_err_handshake(self):
        server = SinkServer(sinker_instance=err_udsink_handler)
        my_servicer = server.servicer
        services = {sink_pb2.DESCRIPTOR.services_by_name["Sink"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        test_datums = [
            sink_pb2.SinkRequest(
                request=sink_pb2.SinkRequest.Request(
                    id="test_id_0",
                    value=mock_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                )
            ),
            sink_pb2.SinkRequest(status=sink_pb2.TransmissionStatus(eot=True)),
        ]

        method = self.test_server.invoke_stream_stream(
            method_descriptor=(
                sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["SinkFn"]
            ),
            invocation_metadata={},
            timeout=1,
        )

        method.send_request(test_datums[0])

        metadata, code, details = method.termination()
        self.assertTrue("UDSinkError: Exception('SinkFn: expected handshake message')" in details)
        self.assertEqual(StatusCode.UNKNOWN, code)

    def test_udsink_err(self):
        server = SinkServer(sinker_instance=err_udsink_handler)
        my_servicer = server.servicer
        services = {sink_pb2.DESCRIPTOR.services_by_name["Sink"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        test_datums = [
            sink_pb2.SinkRequest(
                handshake=sink_pb2.Handshake(sot=True),
            ),
            sink_pb2.SinkRequest(
                request=sink_pb2.SinkRequest.Request(
                    id="test_id_0",
                    value=mock_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                )
            ),
            sink_pb2.SinkRequest(
                request=sink_pb2.SinkRequest.Request(
                    id="test_id_1",
                    value=mock_err_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                )
            ),
            sink_pb2.SinkRequest(status=sink_pb2.TransmissionStatus(eot=True)),
        ]

        method = self.test_server.invoke_stream_stream(
            method_descriptor=(
                sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["SinkFn"]
            ),
            invocation_metadata={},
            timeout=1,
        )

        method.send_request(test_datums[0])
        method.send_request(test_datums[1])
        method.send_request(test_datums[2])
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
        print(code)
        self.assertEqual(StatusCode.UNKNOWN, code)

    def test_forward_message(self):
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        test_datums = [
            sink_pb2.SinkRequest(
                handshake=sink_pb2.Handshake(sot=True),
            ),
            sink_pb2.SinkRequest(
                request=sink_pb2.SinkRequest.Request(
                    id="test_id_0",
                    value=mock_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                )
            ),
            sink_pb2.SinkRequest(
                request=sink_pb2.SinkRequest.Request(
                    id="test_id_1",
                    value=mock_err_message(),
                    event_time=event_time_timestamp,
                    watermark=watermark_timestamp,
                )
            ),
            sink_pb2.SinkRequest(status=sink_pb2.TransmissionStatus(eot=True)),
        ]

        method = self.test_server.invoke_stream_stream(
            method_descriptor=(
                sink_pb2.DESCRIPTOR.services_by_name["Sink"].methods_by_name["SinkFn"]
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

        # 1 handshake +  1 data messages + 1 EOT
        self.assertEqual(3, len(responses))
        # first message should be handshake response
        self.assertTrue(responses[0].handshake.sot)

        # assert the values for the corresponding messages
        self.assertEqual("test_id_0", responses[1].results[0].id)
        self.assertEqual("test_id_1", responses[1].results[1].id)
        self.assertEqual(responses[1].results[0].status, sink_pb2.Status.SUCCESS)
        self.assertEqual(responses[1].results[1].status, sink_pb2.Status.FAILURE)
        self.assertEqual("", responses[1].results[0].err_msg)
        self.assertEqual("mock sink message error", responses[1].results[1].err_msg)

        # last message should be EOT response
        self.assertTrue(responses[2].status.eot)

        _, code, _ = method.termination()
        self.assertEqual(code, StatusCode.OK)

    def test_invalid_init(self):
        with self.assertRaises(TypeError):
            SinkServer()

    @mockenv(NUMAFLOW_UD_CONTAINER_TYPE=UD_CONTAINER_FALLBACK_SINK)
    def test_start_fallback_sink(self):
        server = SinkServer(sinker_instance=udsink_handler)
        self.assertEqual(server.sock_path, f"unix://{FALLBACK_SINK_SOCK_PATH}")
        self.assertEqual(server.server_info_file, FALLBACK_SINK_SERVER_INFO_FILE_PATH)

    def test_max_threads(self):
        # max cap at 16
        server = SinkServer(sinker_instance=udsink_handler, max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = SinkServer(sinker_instance=udsink_handler, max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = SinkServer(sinker_instance=udsink_handler)
        self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    unittest.main()
