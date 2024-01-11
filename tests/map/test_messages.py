import unittest

from pynumaflow.mapper import Messages, Message, DROP, MapperClass, Datum
from tests.testing_utils import mock_message


class TestMessage(unittest.TestCase):
    def test_key(self):
        mock_obj = {"Keys": ["test-key"], "Value": mock_message()}
        msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"])
        print(msg)
        self.assertEqual(mock_obj["Keys"], msg.keys)

    def test_value(self):
        mock_obj = {"Keys": ["test-key"], "Value": mock_message()}
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
        self.assertEqual(len(mock_obj), len(msgs))
        self.assertEqual(len(mock_obj), len(msgs.items()))
        self.assertEqual(mock_obj[0]["Keys"], msgs[0]["Keys"])
        self.assertEqual(mock_obj[0]["Value"], msgs[0]["Value"])
        self.assertEqual(
            "[{'Keys': ['test_key'], 'Value': b'test_mock_message'}, "
            "{'Keys': ['test_key'], 'Value': b'test_mock_message'}]",
            repr(msgs),
        )

    def test_append(self):
        msgs = Messages()
        self.assertEqual(0, len(msgs))
        msgs.append(self.mock_message_object())
        self.assertEqual(1, len(msgs))
        msgs.append(self.mock_message_object())
        self.assertEqual(2, len(msgs))

    def test_message_forward_to_drop(self):
        mock_obj = Messages()
        mock_obj.append(Message(b"").to_drop())
        true_obj = Messages()
        true_obj.append(mock_obj[0])
        self.assertEqual(type(mock_obj), type(true_obj))
        for i in range(len(true_obj)):
            self.assertEqual(type(mock_obj[i]), type(true_obj[i]))
            self.assertEqual(mock_obj[i].keys, true_obj[i].keys)
            self.assertEqual(mock_obj[i].value, true_obj[i].value)
        for msg in true_obj:
            print(msg)

    def test_err(self):
        msgts = Messages(self.mock_message_object(), self.mock_message_object())
        with self.assertRaises(TypeError):
            msgts[:1]


class ExampleMapper(MapperClass):
    def handler(self, keys: list[str], datum: Datum) -> Messages:
        messages = Messages()
        messages.append(Message(mock_message(), keys=keys))
        return messages


class TestMapClass(unittest.TestCase):
    def setUp(self) -> None:
        # Create a map class instance
        self.mapper_instance = ExampleMapper()

    def test_map_class_call(self):
        """Test that the __call__ functionality for the class works,
        ie the class instance can be called directly to invoke the handler function
        """
        # make a call to the class directly
        ret = self.mapper_instance([], None)
        self.assertEqual(mock_message(), ret[0].value)
        # make a call to the handler
        ret_handler = self.mapper_instance.handler(keys=[], datum=None)
        #
        self.assertEqual(ret[0], ret_handler[0])


if __name__ == "__main__":
    unittest.main()
