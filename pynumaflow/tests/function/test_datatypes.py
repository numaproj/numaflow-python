import unittest
from datetime import datetime, timezone


from pynumaflow.function._dtypes import (
    Datum,
    IntervalWindow,
    Metadata,
)


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
    def test_value(self):
        d = Datum(value=mock_message(), event_time=mock_event_time(), water_mark=mock_watermark())
        self.assertEqual(mock_message(), d.value())

    def test_event_time(self):
        d = Datum(value=mock_message(), event_time=mock_event_time(), water_mark=mock_watermark())
        self.assertEqual(mock_event_time(), d.event_time())

    def test_watermark(self):
        d = Datum(value=mock_message(), event_time=mock_event_time(), water_mark=mock_watermark())
        self.assertEqual(mock_watermark(), d.event_time())


class TestIntervalWindow(unittest.TestCase):
    def test_start(self):
        i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
        self.assertEqual(mock_start_time(), i.start())

    def test_end(self):
        i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
        self.assertEqual(mock_end_time(), i.end())


class TestMetadata(unittest.TestCase):
    def test_interval_window(self):
        i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
        m = Metadata(interval_window=i)
        self.assertEqual(type(i), type(m.interval_window()))
        self.assertEqual(i, m.interval_window())


if __name__ == "__main__":
    unittest.main()
