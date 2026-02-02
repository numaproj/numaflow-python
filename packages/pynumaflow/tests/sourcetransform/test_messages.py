import unittest
from datetime import datetime, timezone

from pynumaflow.sourcetransformer import (
    Messages,
    Message,
    DROP,
    SourceTransformer,
    Datum,
    UserMetadata,
    SystemMetadata,
)
from tests.testing_utils import mock_new_event_time


def mock_message_t():
    msgt = bytes("test_mock_message_t", encoding="utf-8")
    return msgt


def mock_event_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


class TestMessage(unittest.TestCase):
    def test_Message_creation(self):
        mock_obj = {
            "Keys": ["test_key"],
            "Value": mock_message_t(),
            "EventTime": mock_event_time(),
            "Tags": ["test_tag"],
        }
        msgt = Message(
            mock_obj["Value"], mock_obj["EventTime"], keys=mock_obj["Keys"], tags=mock_obj["Tags"]
        )
        self.assertEqual(mock_obj["EventTime"], msgt.event_time)
        self.assertEqual(mock_obj["Value"], msgt.value)
        self.assertEqual(mock_obj["Keys"], msgt.keys)
        self.assertEqual(mock_obj["Tags"], msgt.tags)

    def test_message_to_drop(self):
        mock_obj = {
            "Keys": [],
            "Value": b"",
            "Tags": [DROP],
            "EventTime": mock_event_time(),
        }
        msgt = Message(b"", datetime(1, 1, 1, 0, 0)).to_drop(mock_event_time())
        self.assertEqual(Message, type(msgt))
        self.assertEqual(mock_obj["Keys"], msgt.keys)
        self.assertEqual(mock_obj["Value"], msgt.value)
        self.assertEqual(mock_obj["Tags"], msgt.tags)
        self.assertEqual(mock_obj["EventTime"], msgt.event_time)

    def test_message_with_user_metadata(self):
        user_meta = UserMetadata()
        user_meta.add_key("group1", "key1", b"value1")
        user_meta.add_key("group1", "key2", b"value2")

        msgt = Message(
            mock_message_t(),
            mock_event_time(),
            keys=["test_key"],
            user_metadata=user_meta,
        )
        self.assertEqual(mock_message_t(), msgt.value)
        self.assertEqual(["test_key"], msgt.keys)
        self.assertEqual(b"value1", msgt.user_metadata.value("group1", "key1"))
        self.assertEqual(b"value2", msgt.user_metadata.value("group1", "key2"))
        self.assertEqual(["group1"], msgt.user_metadata.groups())

    def test_message_default_user_metadata(self):
        msgt = Message(mock_message_t(), mock_event_time())
        self.assertIsNotNone(msgt.user_metadata)
        self.assertEqual(0, len(msgt.user_metadata))


class TestMessages(unittest.TestCase):
    @staticmethod
    def mock_Message_object():
        value = mock_message_t()
        event_time = mock_event_time()
        return Message(value=value, event_time=event_time)

    def test_items(self):
        mock_obj = [
            {
                "Keys": [b"U+005C__ALL__"],
                "Value": mock_message_t(),
                "EventTime": mock_event_time(),
            },
            {
                "Keys": [b"U+005C__ALL__"],
                "Value": mock_message_t(),
                "EventTime": mock_event_time(),
            },
        ]
        msgts = Messages(*mock_obj)
        self.assertEqual(len(mock_obj), len(msgts))
        self.assertEqual(len(mock_obj), len(msgts.items()))
        self.assertEqual(mock_obj[0]["Keys"], msgts[0]["Keys"])
        self.assertEqual(mock_obj[0]["Value"], msgts[0]["Value"])
        self.assertEqual(mock_obj[0]["EventTime"], msgts[0]["EventTime"])
        self.assertEqual(
            "[{'Keys': [b'U+005C__ALL__'], 'Value': b'test_mock_message_t', "
            "'EventTime': datetime.datetime(2022, 9, 12, 16, 0, tzinfo=datetime.timezone.utc)}, "
            "{'Keys': [b'U+005C__ALL__'], 'Value': b'test_mock_message_t', "
            "'EventTime': datetime.datetime(2022, 9, 12, 16, 0, tzinfo=datetime.timezone.utc)}]",
            repr(msgts),
        )

    def test_append(self):
        msgts = Messages()
        self.assertEqual(0, len(msgts))
        msgts.append(self.mock_Message_object())
        self.assertEqual(1, len(msgts))
        msgts.append(self.mock_Message_object())
        self.assertEqual(2, len(msgts))

    def test_err(self):
        msgts = Messages(self.mock_Message_object(), self.mock_Message_object())
        with self.assertRaises(TypeError):
            msgts[:1]


class TestDatum(unittest.TestCase):
    def test_datum_with_metadata(self):
        user_meta = UserMetadata()
        user_meta.add_key("group1", "key1", b"value1")

        sys_meta = SystemMetadata({"sys_group": {"sys_key": b"sys_value"}})

        d = Datum(
            keys=["test_key"],
            value=mock_message_t(),
            event_time=mock_event_time(),
            watermark=mock_event_time(),
            headers={"header1": "value1"},
            user_metadata=user_meta,
            system_metadata=sys_meta,
        )
        self.assertEqual(["test_key"], d.keys)
        self.assertEqual(mock_message_t(), d.value)
        self.assertEqual(mock_event_time(), d.event_time)
        self.assertEqual({"header1": "value1"}, d.headers)
        self.assertEqual(b"value1", d.user_metadata.value("group1", "key1"))
        self.assertEqual(b"sys_value", d.system_metadata.value("sys_group", "sys_key"))

    def test_datum_default_metadata(self):
        d = Datum(
            keys=["test_key"],
            value=mock_message_t(),
            event_time=mock_event_time(),
            watermark=mock_event_time(),
        )
        self.assertIsNotNone(d.user_metadata)
        self.assertIsNotNone(d.system_metadata)
        self.assertEqual(0, len(d.user_metadata))
        self.assertEqual([], d.system_metadata.groups())


class ExampleSourceTransformClass(SourceTransformer):
    def handler(self, keys: list[str], datum: Datum) -> Messages:
        messages = Messages()
        messages.append(Message(mock_message_t(), mock_new_event_time(), keys=keys))
        return messages


class TestSourceTransformClass(unittest.TestCase):
    def setUp(self) -> None:
        # Create a map class instance
        self.transform_instance = ExampleSourceTransformClass()

    def test_source_transform_class_call(self):
        """Test that the __call__ functionality for the class works,
        ie the class instance can be called directly to invoke the handler function
        """
        # make a call to the class directly
        ret = self.transform_instance([], None)
        self.assertEqual(mock_message_t(), ret[0].value)
        # make a call to the handler
        ret_handler = self.transform_instance.handler([], None)
        # Both responses should be equal
        self.assertEqual(ret[0], ret_handler[0])


if __name__ == "__main__":
    unittest.main()
