import unittest
from datetime import datetime, timezone

from pynumaflow.sourcetransformer import Messages, Message, DROP, EVENT_TIME_TO_DROP


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
            "EventTime": EVENT_TIME_TO_DROP,
        }
        msgt = Message(b"", mock_event_time()).to_drop()
        self.assertEqual(Message, type(msgt))
        self.assertEqual(mock_obj["Keys"], msgt.keys)
        self.assertEqual(mock_obj["Value"], msgt.value)
        self.assertEqual(mock_obj["Tags"], msgt.tags)
        self.assertEqual(mock_obj["EventTime"], msgt.event_time)


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


if __name__ == "__main__":
    unittest.main()
