import unittest
from datetime import datetime, timezone

from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow.function._dtypes import (
    IntervalWindow,
    Metadata,
    Datum,
    DatumMetadata,
)

TEST_KEYS = ["test"]
TEST_ID = "test_id"


def mock_message():
    msg = bytes("test_mock_message", encoding="utf-8")
    return msg


def mock_event_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


def mock_watermark():
    t = datetime.fromtimestamp(1662998460, timezone.utc)
    return t


def mock_start_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


def mock_end_time():
    t = datetime.fromtimestamp(1662998520, timezone.utc)
    return t


class TestDatum(unittest.TestCase):
    def test_err_event_time(self):
        ts = _timestamp_pb2.Timestamp()
        ts.GetCurrentTime()
        metadata = DatumMetadata(
            msg_id=TEST_ID,
            num_delivered=1,
        )
        with self.assertRaises(Exception) as context:
            Datum(
                keys=TEST_KEYS, value=mock_message(), event_time=ts, watermark=ts, metadata=metadata
            )
        self.assertEqual(
            "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> "
            "for Datum.event_time",
            str(context.exception),
        )

    def test_err_watermark(self):
        ts = _timestamp_pb2.Timestamp()
        ts.GetCurrentTime()
        metadata = DatumMetadata(
            msg_id=TEST_ID,
            num_delivered=1,
        )
        with self.assertRaises(Exception) as context:
            Datum(
                keys=TEST_KEYS,
                value=mock_message(),
                event_time=mock_event_time(),
                watermark=ts,
                metadata=metadata,
            )
        self.assertEqual(
            "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> "
            "for Datum.watermark",
            str(context.exception),
        )

    def test_value(self):
        metadata = DatumMetadata(
            msg_id=TEST_ID,
            num_delivered=1,
        )
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            metadata=metadata,
        )
        self.assertEqual(mock_message(), d.value)

    def test_key(self):
        metadata = DatumMetadata(
            msg_id=TEST_ID,
            num_delivered=1,
        )
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            metadata=metadata,
        )
        self.assertEqual(TEST_KEYS, d.keys())

    def test_event_time(self):
        metadata = DatumMetadata(
            msg_id=TEST_ID,
            num_delivered=1,
        )
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            metadata=metadata,
        )
        self.assertEqual(mock_event_time(), d.event_time)

    def test_watermark(self):
        metadata = DatumMetadata(
            msg_id=TEST_ID,
            num_delivered=1,
        )
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            metadata=metadata,
        )
        self.assertEqual(mock_watermark(), d.watermark)

    def test_id(self):
        metadata = DatumMetadata(
            msg_id=TEST_ID,
            num_delivered=1,
        )
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            metadata=metadata,
        )
        self.assertEqual(TEST_ID, d.metadata.id)

    def test_num_delivered(self):
        metadata = DatumMetadata(
            msg_id=TEST_ID,
            num_delivered=1,
        )
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            metadata=metadata,
        )
        self.assertEqual(1, d.metadata.num_delivered)


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
