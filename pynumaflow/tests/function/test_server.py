import json
import multiprocessing
import os
import tempfile
import time
import unittest
from datetime import datetime, timezone

from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import empty_pb2 as _empty_pb2


from pynumaflow._constants import DATUM_KEY
from pynumaflow.function import udfunction_pb2_grpc, udfunction_pb2
from pynumaflow.function.server import UserDefinedFunctionServicer
from pynumaflow.function._dtypes import (
    Message,
    Messages,
    Datum,
)


def map_handler(key: str, datum: Datum) -> Messages:
    val = datum.value()
    msg = "payload:%s event_time:%s watermark:%s" % (
        val.decode("utf-8"),
        datum.event_time(),
        datum.watermark(),
    )
    val = bytes(msg, encoding="utf-8")
    messages = Messages()
    messages.append(Message.to_vtx(key, val))
    return messages


def mock_message():
    msg = bytes("test_mock_message", encoding="utf-8")
    return msg


def mock_event_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


def mock_watermark():
    t = datetime.fromtimestamp(1662998460, timezone.utc)
    return t


class TestServer(unittest.TestCase):
    def __init__(self, method_name) -> None:
        super().__init__(method_name)

        my_servicer = UserDefinedFunctionServicer(map_handler)
        services = {udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

    async def test_start(self):
        # TODO: not sure if this is the correct way to test..
        with tempfile.TemporaryDirectory() as tmp_dir:
            server = UserDefinedFunctionServicer(
                map_handler, sock_path="unix://%s/numaflow-test.sock" % tmp_dir
            )
            server.start()

    def test_is_ready(self):
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"].methods_by_name[
                    "IsReady"
                ]
            ),
            invocation_metadata={DATUM_KEY, "test"},
            request=_empty_pb2.Empty(),
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        expected = udfunction_pb2.ReadyResponse(ready=True)
        self.assertEqual(expected, response)
        self.assertEqual(code, StatusCode.OK)

    def test_forward_message(self):
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.Datum(
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
            invocation_metadata={(DATUM_KEY, "test")},
            request=request,
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        self.assertEqual(1, len(response.elements))
        self.assertEqual("test", response.elements[0].key)
        self.assertEqual(
            bytes(
                "payload:test_mock_message event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                encoding="utf-8",
            ),
            response.elements[0].value,
        )
        self.assertEqual(code, StatusCode.OK)


if __name__ == "__main__":
    unittest.main()
