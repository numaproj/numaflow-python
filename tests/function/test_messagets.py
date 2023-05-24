import unittest
from datetime import datetime, timezone

from pynumaflow.function import MessageTs, MessageT, DROP


def mock_message_t():
    msgt = bytes("test_mock_message_t", encoding="utf-8")
    return msgt


def mock_event_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


class TestMessageT(unittest.TestCase):
    def test_messageT_creation(self):
        mock_obj = {
            "Keys": ["test_key"],
            "Value": mock_message_t(),
            "EventTime": mock_event_time(),
            "Tags": ["test_tag"],
        }
        msgt = MessageT(
            mock_obj["Value"], mock_obj["EventTime"], keys=mock_obj["Keys"], tags=mock_obj["Tags"]
        )
        self.assertEqual(mock_obj["EventTime"], msgt.event_time)
        self.assertEqual(mock_obj["Value"], msgt.value)
        self.assertEqual(mock_obj["Keys"], msgt.keys)
        self.assertEqual(mock_obj["Tags"], msgt.tags)

    def test_message_to_drop(self):
        mock_obj = {"Keys": [], "Value": b"", "Tags": [DROP]}
        msgt = MessageT(b"", mock_event_time()).to_drop()
        self.assertEqual(MessageT, type(msgt))
        self.assertEqual(mock_obj["Keys"], msgt.keys)
        self.assertEqual(mock_obj["Value"], msgt.value)
        self.assertEqual(mock_obj["Tags"], msgt.tags)


class TestMessageTs(unittest.TestCase):
    @staticmethod
    def mock_messaget_object():
        value = mock_message_t()
        event_time = mock_event_time()
        return MessageT(value=value, event_time=event_time)

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
        msgts = MessageTs(*mock_obj)
        self.assertEqual(len(mock_obj), len(msgts.items()))
        self.assertEqual(mock_obj[0]["Keys"], msgts.items()[0]["Keys"])
        self.assertEqual(mock_obj[0]["Value"], msgts.items()[0]["Value"])
        self.assertEqual(mock_obj[0]["EventTime"], msgts.items()[0]["EventTime"])
        self.assertEqual(
            "[{'Keys': [b'U+005C__ALL__'], 'Value': b'test_mock_message_t', "
            "'EventTime': datetime.datetime(2022, 9, 12, 16, 0, tzinfo=datetime.timezone.utc)}, "
            "{'Keys': [b'U+005C__ALL__'], 'Value': b'test_mock_message_t', "
            "'EventTime': datetime.datetime(2022, 9, 12, 16, 0, tzinfo=datetime.timezone.utc)}]",
            repr(msgts),
        )

    def test_append(self):
        msgts = MessageTs()
        self.assertEqual(0, len(msgts.items()))
        msgts.append(self.mock_messaget_object())
        self.assertEqual(1, len(msgts.items()))
        msgts.append(self.mock_messaget_object())
        self.assertEqual(2, len(msgts.items()))

    def test_dump(self):
        msgts = MessageTs()
        msgts.append(self.mock_messaget_object())
        msgts.append(self.mock_messaget_object())
        self.assertEqual(
            "[MessageT(_keys=[], _tags=[], _value=b'test_mock_message_t', "
            "_event_time=datetime.datetime(2022, 9, 12, 16, 0, tzinfo=datetime.timezone.utc)), "
            "MessageT(_keys=[], _tags=[], _value=b'test_mock_message_t', "
            "_event_time=datetime.datetime(2022, 9, 12, 16, 0, tzinfo=datetime.timezone.utc))]",
            msgts.dumps(),
        )

    def test_load(self):
        # to improve codecov
        msgts = MessageTs()
        msgts.loads()


if __name__ == "__main__":
    unittest.main()
