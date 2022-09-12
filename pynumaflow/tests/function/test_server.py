import json
import os
import tempfile
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


def test_map_handler(key: str, datum: Datum) -> Messages:
    val = datum.value()
    val = val.decode("utf-8") + "_map_function"
    val = bytes(val, encoding="utf-8")
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

        my_servicer = UserDefinedFunctionServicer(test_map_handler)
        services = {udfunction_pb2.DESCRIPTOR.services_by_name["UserDefinedFunction"]: my_servicer}
        self.test_server = server_from_dictionary(services, strict_real_time())

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
        timestamp = _timestamp_pb2.Timestamp()

        request = udfunction_pb2.Datum(
            value=mock_message(),
            event_time=timestamp.FromDatetime(dt=mock_event_time()),
            watermark=timestamp.FromDatetime(dt=mock_watermark()),
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
        self.assertEqual("test", response.elements[0].key)
        self.assertEqual(
            bytes("test_mock_message_map_function", encoding="utf-8"), response.elements[0].value
        )
        # self.assertTupleEqual("test_forward_message", response.__dict__.[])
        self.assertEqual(code, StatusCode.OK)


if __name__ == "__main__":
    unittest.main()
