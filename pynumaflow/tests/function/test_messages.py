import unittest

from pynumaflow.function._dtypes import (
    Message,
    Messages,
    ALL,
    DROP,
)


def mock_message():
    msg = bytes("test_mock_message", encoding="utf-8")
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
        mock_obj = {"Key": "__KEY__", "Value": mock_message()}
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
        self.assertEqual(
            "[{'Key': b'U+005C__ALL__', 'Value': b'test_mock_message'}, "
            "{'Key': b'U+005C__ALL__', 'Value': b'test_mock_message'}]",
            repr(msgs),
        )

    def test_append(self):
        msgs = Messages()
        self.assertEqual(0, len(msgs.items()))
        msgs.append(self.mock_message_object())
        self.assertEqual(1, len(msgs.items()))
        msgs.append(self.mock_message_object())
        self.assertEqual(2, len(msgs.items()))

    def test_message_forward(self):
        mock_obj = Messages()
        msg = self.mock_message_object()
        mock_obj.append(msg)
        true_obj = Messages.as_forward_all(mock_message())
        self.assertEqual(type(mock_obj), type(true_obj))
        for i in range(len(true_obj.items())):
            self.assertEqual(type(mock_obj.items()[i]), type(true_obj.items()[i]))
            self.assertEqual(mock_obj.items()[i].key, true_obj.items()[i].key)
            self.assertEqual(mock_obj.items()[i].value, true_obj.items()[i].value)

    def test_message_forward_to_drop(self):
        mock_obj = Messages()
        mock_obj.append(Message(DROP, bytes()))
        true_obj = Messages.as_forward_all(bytes())
        self.assertEqual(type(mock_obj), type(true_obj))
        for i in range(len(true_obj.items())):
            self.assertEqual(type(mock_obj.items()[i]), type(true_obj.items()[i]))
            self.assertEqual(mock_obj.items()[i].key, true_obj.items()[i].key)
            self.assertEqual(mock_obj.items()[i].value, true_obj.items()[i].value)

    def test_dump(self):
        msgs = Messages()
        msgs.append(self.mock_message_object())
        msgs.append(self.mock_message_object())
        self.assertEqual(
            "[{b'U+005C__ALL__': b'test_mock_message'}, {b'U+005C__ALL__': b'test_mock_message'}]",
            msgs.dumps(),
        )

    def test_load(self):
        # to improve codecov
        msgs = Messages()
        msgs.loads()


if __name__ == "__main__":
    unittest.main()
