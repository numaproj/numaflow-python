import unittest

from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow.batchmapper._dtypes import (
    Datum,
)
from tests.testing_utils import (
    mock_message,
    mock_event_time,
    mock_watermark,
)

TEST_KEYS = ["test"]
TEST_ID = "test_id"
TEST_HEADERS = {"key1": "value1", "key2": "value2"}


class TestDatum(unittest.TestCase):
    def test_err_event_time(self):
        ts = _timestamp_pb2.Timestamp()
        ts.GetCurrentTime()
        with self.assertRaises(Exception) as context:
            Datum(
                keys=TEST_KEYS,
                value=mock_message(),
                event_time=ts,
                watermark=ts,
                headers=TEST_HEADERS,
                id=TEST_ID,
            )
        self.assertEqual(
            "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> "
            "for Datum.event_time",
            str(context.exception),
        )

    def test_err_watermark(self):
        ts = _timestamp_pb2.Timestamp()
        ts.GetCurrentTime()
        with self.assertRaises(Exception) as context:
            Datum(
                keys=TEST_KEYS,
                value=mock_message(),
                event_time=mock_event_time(),
                watermark=ts,
                headers=TEST_HEADERS,
                id=TEST_ID,
            )
        self.assertEqual(
            "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> "
            "for Datum.watermark",
            str(context.exception),
        )

    def test_value(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            headers=TEST_HEADERS,
            id=TEST_ID,
        )
        self.assertEqual(mock_message(), d.value)

    def test_key(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            id=TEST_ID,
        )
        self.assertEqual(TEST_KEYS, d.keys())

    def test_event_time(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            headers=TEST_HEADERS,
            id=TEST_ID,
        )
        self.assertEqual(mock_event_time(), d.event_time)
        self.assertEqual(TEST_HEADERS, d.headers)

    def test_watermark(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            id=TEST_ID,
        )
        self.assertEqual(mock_watermark(), d.watermark)
        self.assertEqual({}, d.headers)

    def test_id(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            id=TEST_ID,
        )
        self.assertEqual(TEST_ID, d.id)
        self.assertEqual({}, d.headers)


if __name__ == "__main__":
    unittest.main()
