import unittest
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

from pynumaflow.function import MessageTs, MessageT, ALL, DROP


def mock_message_t():
    msgt = bytes("test_mock_message_t", encoding="utf-8")
    return msgt


def mock_event_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


class TestMessageT(unittest.TestCase):
    def test_key(self):
        mock_obj = {"Key": ALL, "Value": mock_message_t(), "EventTime": mock_event_time()}
        msgt = MessageT(
            key=mock_obj["Key"], value=mock_obj["Value"], event_time=mock_obj["EventTime"]
        )
        self.assertEqual(mock_obj["Key"], msgt.key)

    def test_immutable(self):
        msgt = MessageT(ALL, mock_message_t())
        with self.assertRaises(FrozenInstanceError):
            msgt.key = DROP

    def test_value(self):
        mock_obj = {"Key": ALL, "Value": mock_message_t(), "EventTime": mock_event_time()}
        msgts = MessageT(key=mock_obj["Key"], value=mock_obj["Value"])
        self.assertEqual(mock_obj["Value"], msgts.value)

    def test_event_time(self):
        mock_obj = {"Key": ALL, "Value": mock_message_t(), "EventTime": mock_event_time()}
        msgts = MessageT(
            key=mock_obj["Key"], value=mock_obj["Value"], event_time=mock_obj["EventTime"]
        )
        self.assertEqual(mock_obj["EventTime"], msgts.event_time)

    def test_message_to_all(self):
        mock_obj = {"Key": ALL, "Value": mock_message_t(), "EventTime": mock_event_time()}
        msgt = MessageT.to_all(mock_obj["Value"], mock_obj["EventTime"])
        self.assertEqual(MessageT, type(msgt))
        self.assertEqual(mock_obj["Key"], msgt.key)
        self.assertEqual(mock_obj["Value"], msgt.value)
        self.assertEqual(mock_obj["EventTime"], msgt.event_time)

    def test_message_to_drop(self):
        mock_obj = {"Key": DROP, "Value": b""}
        msgt = MessageT.to_drop()
        self.assertEqual(MessageT, type(msgt))
        self.assertEqual(mock_obj["Key"], msgt.key)
        self.assertEqual(mock_obj["Value"], msgt.value)

    def test_message_to(self):
        mock_obj = {"Key": "__KEY__", "Value": mock_message_t(), "EventTime": mock_event_time()}
        msgt = MessageT.to_vtx(
            key=mock_obj["Key"], value=mock_obj["Value"], event_time=mock_obj["EventTime"]
        )
        self.assertEqual(MessageT, type(msgt))
        self.assertEqual(mock_obj["Key"], msgt.key)
        self.assertEqual(mock_obj["Value"], msgt.value)
        self.assertEqual(mock_obj["EventTime"], msgt.event_time)


class TestMessageTs(unittest.TestCase):
    @staticmethod
    def mock_messaget_object():
        value = mock_message_t()
        event_time = mock_event_time()
        return MessageT(ALL, value, event_time)

    def test_items(self):
        mock_obj = [
            {"Key": b"U+005C__ALL__", "Value": mock_message_t(), "EventTime": mock_event_time()},
            {"Key": b"U+005C__ALL__", "Value": mock_message_t(), "EventTime": mock_event_time()},
        ]
        msgts = MessageTs(*mock_obj)
        self.assertEqual(len(mock_obj), len(msgts.items()))
        self.assertEqual(mock_obj[0]["Key"], msgts.items()[0]["Key"])
        self.assertEqual(mock_obj[0]["Value"], msgts.items()[0]["Value"])
        self.assertEqual(mock_obj[0]["EventTime"], msgts.items()[0]["EventTime"])
        self.assertEqual(
            "[{'Key': b'U+005C__ALL__', 'Value': b'test_mock_message_t', 'EventTime': datetime.datetime(2022, 9, 12, "
            "16, 0, tzinfo=datetime.timezone.utc)}, {'Key': b'U+005C__ALL__', 'Value': b'test_mock_message_t', "
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

    def test_message_forward(self):
        mock_obj = MessageTs(self.mock_messaget_object())
        true_obj = MessageTs.as_forward_all(mock_message_t(), mock_event_time())
        self.assertEqual(type(mock_obj), type(true_obj))
        for i in range(len(true_obj.items())):
            self.assertEqual(type(mock_obj.items()[i]), type(true_obj.items()[i]))
            self.assertEqual(mock_obj.items()[i].key, true_obj.items()[i].key)
            self.assertEqual(mock_obj.items()[i].value, true_obj.items()[i].value)
            self.assertEqual(mock_obj.items()[i].event_time, true_obj.items()[i].event_time)

    def test_message_forward_to_drop(self):
        mock_obj = MessageTs()
        mock_obj.append(MessageT(DROP, bytes()))
        true_obj = MessageTs.as_forward_all(bytes(), mock_event_time())
        self.assertEqual(type(mock_obj), type(true_obj))
        for i in range(len(true_obj.items())):
            self.assertEqual(type(mock_obj.items()[i]), type(true_obj.items()[i]))
            self.assertEqual(mock_obj.items()[i].key, true_obj.items()[i].key)
            self.assertEqual(mock_obj.items()[i].value, true_obj.items()[i].value)
            self.assertEqual(mock_obj.items()[i].value, true_obj.items()[i].value)

    def test_dump(self):
        msgts = MessageTs()
        msgts.append(self.mock_messaget_object())
        msgts.append(self.mock_messaget_object())
        self.assertEqual(
            "[MessageT(key=b'U+005C__ALL__', value=b'test_mock_message_t', event_time=datetime.datetime(2022, 9, 12, "
            "16, 0, tzinfo=datetime.timezone.utc)), MessageT(key=b'U+005C__ALL__', value=b'test_mock_message_t', "
            "event_time=datetime.datetime(2022, 9, 12, 16, 0, tzinfo=datetime.timezone.utc))]",
            msgts.dumps(),
        )

    def test_load(self):
        # to improve codecov
        msgts = MessageTs()
        msgts.loads()


if __name__ == "__main__":
    unittest.main()
