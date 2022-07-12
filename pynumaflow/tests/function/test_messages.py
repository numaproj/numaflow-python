import base64
import json
import os
import unittest
from unittest import mock

import msgpack

from pynumaflow.encoder import msgpack_encoding, NumaflowJSONEncoder
from pynumaflow.exceptions import MarshalError
from pynumaflow.function._dtypes import (
    Message,
    Messages,
    ALL,
    DROP,
)


def mock_json():
    msg = {"test": "handler"}
    msg = json.dumps(msg)
    return msg


def mock_message():
    msg = {"test": "handler"}
    msg = json.dumps(msg).encode("utf-8")
    return msg


class TestMessage(unittest.TestCase):
    def test_key(self):
        mock_obj = {"Key": ALL, "Value": mock_message()}
        msgs = Message(key=mock_obj["Key"], value=mock_obj["Value"])
        print(msgs)
        self.assertEqual(mock_obj["Key"], msgs.key)

    def test_value(self):
        mock_obj = {"Key": ALL, "Value": mock_message()}
        msgs = Message(key=mock_obj["Key"], value=mock_obj["Value"])
        self.assertEqual(mock_obj["Value"], msgs.value)

    def test_message_to_all(self):
        mock_obj = {"Key": ALL, "Value": mock_message()}
        msgs = Message.to_all(value=mock_obj["Value"])
        self.assertEqual(Message, type(msgs))
        self.assertEqual(mock_obj["Key"], msgs.key)
        self.assertEqual(mock_obj["Value"], msgs.value)

    def test_message_to_drop(self):
        mock_obj = {"Key": DROP, "Value": b""}
        msgs = Message.to_drop()
        self.assertEqual(Message, type(msgs))
        self.assertEqual(mock_obj["Key"], msgs.key)
        self.assertEqual(mock_obj["Value"], msgs.value)

    def test_message_to(self):
        mock_obj = {"Key": b"__KEY__", "Value": mock_message()}
        msgs = Message.to_vtx(key=mock_obj["Key"], value=mock_obj["Value"])
        self.assertEqual(Message, type(msgs))
        self.assertEqual(mock_obj["Key"], msgs.key)
        self.assertEqual(mock_obj["Value"], msgs.value)


class TestMessages(unittest.TestCase):
    @staticmethod
    def mock_message_object():
        value = mock_message()
        return Message(ALL, value)

    def test_items(self):
        mock_obj = [
            {"Key": b"U+005C__ALL__", "Value": mock_message()},
            {"Key": b"U+005C__ALL__", "Value": mock_message()},
        ]
        msgs = Messages()
        msgs._messages = mock_obj
        self.assertEqual(len(mock_obj), len(msgs.items()))
        self.assertEqual(mock_obj[0]["Key"], msgs.items()[0]["Key"])
        self.assertEqual(mock_obj[0]["Value"], msgs.items()[0]["Value"])

    def test_append(self):
        msgs = Messages()
        self.assertEqual(0, len(msgs.items()))
        msgs.append(self.mock_message_object())
        self.assertEqual(1, len(msgs.items()))
        msgs.append(self.mock_message_object())
        self.assertEqual(2, len(msgs.items()))

    def test_msgpack_encoding(self):
        mock_obj = [
            {"Key": b"U+005C__ALL__", "Value": mock_message()},
            {"Key": b"U+005C__ALL__", "Value": mock_message()},
        ]
        true_obj = [
            msgpack_encoding(self.mock_message_object()),
            msgpack_encoding(self.mock_message_object()),
        ]
        for i in range(len(mock_obj)):
            self.assertEqual(mock_obj[i]["Key"], true_obj[i]["Key"])
            self.assertEqual(mock_obj[i]["Value"], true_obj[i]["Value"])

    def test_nfjsonencoder_default(self):
        mock_obj = {"Key": b"U+005C__ALL__", "Value": mock_message()}
        numaflow_json_encoder = NumaflowJSONEncoder()
        true_obj = numaflow_json_encoder.default(self.mock_message_object())
        self.assertEqual(mock_obj["Value"], true_obj["value"])
        self.assertEqual(mock_obj["Key"], true_obj["key"])

    def test_nfjsonencoder_default_bytes(self):
        mock_obj = base64.b64encode(mock_message()).decode("utf-8")
        numaflow_json_encoder = NumaflowJSONEncoder()
        true_obj = numaflow_json_encoder.default(mock_message())
        self.assertEqual(mock_obj, true_obj)

    def test_message_forward(self):
        mock_obj = Messages()
        msg = self.mock_message_object()
        mock_obj.append(msg)
        true_obj = Messages.as_forward_all(mock_json())
        self.assertEqual(type(mock_obj), type(true_obj))
        for i in range(len(mock_obj.items())):
            self.assertEqual(type(mock_obj.items()[i]), type(true_obj.items()[i]))
            self.assertEqual(mock_obj.items()[i].key, true_obj.items()[i].key)
            self.assertEqual(mock_obj.items()[i].value, true_obj.items()[i].value)

    @mock.patch.dict(os.environ, {"NUMAFLOW_UDF_CONTENT_TYPE": "application/json"}, clear=True)
    def test_marshall_messages_json_content_type(self):

        mock_msg = Messages()
        msg = self.mock_message_object()
        mock_msg.append(msg)
        mock_obj = json.dumps(mock_msg.items(), cls=NumaflowJSONEncoder, separators=(",", ":"))
        true_obj = mock_msg.dumps(os.environ.get("NUMAFLOW_UDF_CONTENT_TYPE"))
        self.assertEqual(os.environ.get("NUMAFLOW_UDF_CONTENT_TYPE"), "application/json")
        self.assertEqual(type(mock_obj), type(true_obj))
        self.assertEqual(mock_obj, true_obj)

    @mock.patch.dict(os.environ, {"NUMAFLOW_UDF_CONTENT_TYPE": "application/msgpack"}, clear=True)
    def test_marshall_messages_msgpack_content_type(self):

        mock_msgs = Messages()
        msg = self.mock_message_object()
        mock_msgs.append(msg)
        mock_obj = msgpack.dumps(mock_msgs.items(), default=msgpack_encoding)
        true_obj = mock_msgs.dumps(os.environ.get("NUMAFLOW_UDF_CONTENT_TYPE"))
        self.assertEqual(os.environ.get("NUMAFLOW_UDF_CONTENT_TYPE"), "application/msgpack")
        self.assertEqual(type(mock_obj), type(true_obj))
        self.assertEqual(mock_obj, true_obj)

    @mock.patch.dict(os.environ, {"NUMAFLOW_UDF_CONTENT_TYPE": "x"}, clear=True)
    def test_marshall_messages_unknown_content_type(self):

        mock_msgs = Messages()
        msg = self.mock_message_object()
        mock_msgs.append(msg)
        self.assertEqual(os.environ.get("NUMAFLOW_UDF_CONTENT_TYPE"), "x")
        print(mock_msgs)
        with self.assertRaises(MarshalError):
            mock_msgs.dumps(os.environ.get("NUMAFLOW_UDF_CONTENT_TYPE"))


if __name__ == "__main__":
    unittest.main()
