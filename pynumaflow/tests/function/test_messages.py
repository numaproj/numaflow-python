import unittest

from pynumaflow.function import Messages, Message, DROP


def mock_message():
    msg = bytes("test_mock_message", encoding="utf-8")
    return msg


class TestMessage(unittest.TestCase):
    def test_key(self):
        mock_obj = {"Keys": "test-key", "Value": mock_message()}
        msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"])
        print(msg)
        self.assertEqual(mock_obj["Keys"], msg.keys)

    def test_value(self):
        mock_obj = {"Keys": "test-key", "Value": mock_message()}
        msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"])
        self.assertEqual(mock_obj["Value"], msg.value)

    def test_message_to_all(self):
        mock_obj = {"Keys": [], "Value": mock_message(), "Tags": []}
        msg = Message(mock_obj["Value"])
        self.assertEqual(Message, type(msg))
        self.assertEqual(mock_obj["Keys"], msg.keys)
        self.assertEqual(mock_obj["Value"], msg.value)
        self.assertEqual(mock_obj["Tags"], msg.tags)

    def test_message_to_drop(self):
        mock_obj = {"Keys": [], "Value": b"", "Tags": [DROP]}
        msg = Message(b"").to_drop()
        self.assertEqual(Message, type(msg))
        self.assertEqual(mock_obj["Keys"], msg.keys)
        self.assertEqual(mock_obj["Value"], msg.value)
        self.assertEqual(mock_obj["Tags"], msg.tags)

    def test_message_to(self):
        mock_obj = {"Keys": ["__KEY__"], "Value": mock_message(), "Tags": ["__TAG__"]}
        msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"], tags=mock_obj["Tags"])
        self.assertEqual(Message, type(msg))
        self.assertEqual(mock_obj["Keys"], msg.keys)
        self.assertEqual(mock_obj["Value"], msg.value)
        self.assertEqual(mock_obj["Tags"], msg.tags)


class TestMessages(unittest.TestCase):
    @staticmethod
    def mock_message_object():
        value = mock_message()
        return Message(value=value)

    def test_items(self):
        mock_obj = [
            {"Keys": ["test_key"], "Value": mock_message()},
            {"Keys": ["test_key"], "Value": mock_message()},
        ]
        msgs = Messages(*mock_obj)
        self.assertEqual(len(mock_obj), len(msgs.items()))
        self.assertEqual(mock_obj[0]["Keys"], msgs.items()[0]["Keys"])
        self.assertEqual(mock_obj[0]["Value"], msgs.items()[0]["Value"])
        self.assertEqual(
            "[{'Keys': ['test_key'], 'Value': b'test_mock_message'}, "
            "{'Keys': ['test_key'], 'Value': b'test_mock_message'}]",
            repr(msgs),
        )

    def test_append(self):
        msgs = Messages()
        self.assertEqual(0, len(msgs.items()))
        msgs.append(self.mock_message_object())
        self.assertEqual(1, len(msgs.items()))
        msgs.append(self.mock_message_object())
        self.assertEqual(2, len(msgs.items()))

    def test_message_forward_to_drop(self):
        mock_obj = Messages()
        mock_obj.append(Message(b"").to_drop())
        true_obj = Messages()
        true_obj.append(mock_obj.items()[0])
        self.assertEqual(type(mock_obj), type(true_obj))
        for i in range(len(true_obj.items())):
            self.assertEqual(type(mock_obj.items()[i]), type(true_obj.items()[i]))
            self.assertEqual(mock_obj.items()[i].keys, true_obj.items()[i].keys)
            self.assertEqual(mock_obj.items()[i].value, true_obj.items()[i].value)

    def test_dump(self):
        msgs = Messages()
        msgs.append(self.mock_message_object())
        msgs.append(self.mock_message_object())
        self.assertEqual(
            "[Message(_keys=[], _tags=[], _value=b'test_mock_message'), "
            "Message(_keys=[], _tags=[], _value=b'test_mock_message')]",
            msgs.dumps(),
        )

    def test_load(self):
        # to improve codecov
        msgs = Messages()
        msgs.loads()


if __name__ == "__main__":
    unittest.main()
