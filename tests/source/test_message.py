import unittest

from pynumaflow.sourcer import Message, Offset, Datum
from tests.source.utils import mock_offset
from tests.testing_utils import mock_event_time


class TestMessage(unittest.TestCase):
    def test_message_creation(self):
        payload = b"payload:test_mock_message"
        keys = ["test_key"]
        offset = mock_offset()
        event_time = mock_event_time()
        msg = Message(payload=payload, offset=offset, keys=keys, event_time=event_time)
        self.assertEqual(event_time, msg.event_time)
        self.assertEqual(payload, msg.payload)
        self.assertEqual(keys, msg.keys)
        self.assertEqual(offset, msg.offset)


class TestOffset(unittest.TestCase):
    def test_offset_creation(self):
        msg = Offset(offset=mock_offset().offset, partition_id=mock_offset().partition_id)
        self.assertEqual(msg.offset, mock_offset().offset)
        self.assertEqual(msg.partition_id, mock_offset().partition_id)


class TestDatum(unittest.TestCase):
    def test_datum_creation(self):
        msg = Datum(num_records=1, timeout_in_ms=1000)
        self.assertEqual(msg.num_records, 1)
        self.assertEqual(msg.timeout_in_ms, 1000)

    def test_err_num_record(self):
        try:
            Datum(num_records="HEKKO", timeout_in_ms=1000)
        except TypeError as e:
            self.assertTrue("Wrong data type" in e.__str__())
            return
        self.fail("Expected TypeError")

    def test_err_timeout(self):
        try:
            Datum(num_records=1, timeout_in_ms="1000")
        except TypeError as e:
            self.assertTrue("Wrong data type" in e.__str__())
            return
        self.fail("Expected TypeError")


if __name__ == "__main__":
    unittest.main()
