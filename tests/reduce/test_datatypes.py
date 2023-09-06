import unittest

from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow.reducer._dtypes import (
    IntervalWindow,
    Metadata,
    Datum,
)
from tests.testing_utils import (
    mock_message,
    mock_event_time,
    mock_watermark,
    mock_start_time,
    mock_end_time,
)

TEST_KEYS = ["test"]
TEST_ID = "test_id"


class TestDatum(unittest.TestCase):
    def test_err_event_time(self):
        ts = _timestamp_pb2.Timestamp()
        ts.GetCurrentTime()
        with self.assertRaises(Exception) as context:
            Datum(keys=TEST_KEYS, value=mock_message(), event_time=ts, watermark=ts)
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
        )
        self.assertEqual(mock_message(), d.value)

    def test_key(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
        )
        self.assertEqual(TEST_KEYS, d.keys())

    def test_event_time(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
        )
        self.assertEqual(mock_event_time(), d.event_time)

    def test_watermark(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
        )
        self.assertEqual(mock_watermark(), d.watermark)


class TestIntervalWindow(unittest.TestCase):
    def test_start(self):
        i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
        self.assertEqual(mock_start_time(), i.start)

    def test_end(self):
        i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
        self.assertEqual(mock_end_time(), i.end)


class TestMetadata(unittest.TestCase):
    def test_interval_window(self):
        i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
        m = Metadata(interval_window=i)
        self.assertEqual(type(i), type(m.interval_window))
        self.assertEqual(i, m.interval_window)


if __name__ == "__main__":
    unittest.main()
