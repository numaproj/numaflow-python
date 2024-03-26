from copy import deepcopy
import unittest
from collections.abc import AsyncIterable

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from pynumaflow.reducer import Reducer, Messages

from pynumaflow.reducer._dtypes import (
    IntervalWindow,
    Metadata,
    Datum,
    ReduceWindow,
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
        headers = {"key1": "value1", "key2": "value2"}
        with self.assertRaises(Exception) as context:
            Datum(
                keys=TEST_KEYS, value=mock_message(), event_time=ts, watermark=ts, headers=headers
            )
        self.assertEqual(
            "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> "
            "for Datum.event_time",
            str(context.exception),
        )

    def test_err_watermark(self):
        ts = _timestamp_pb2.Timestamp()
        ts.GetCurrentTime()
        headers = {"key1": "value1", "key2": "value2"}
        with self.assertRaises(Exception) as context:
            Datum(
                keys=TEST_KEYS,
                value=mock_message(),
                event_time=mock_event_time(),
                watermark=ts,
                headers=headers,
            )
        self.assertEqual(
            "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> "
            "for Datum.watermark",
            str(context.exception),
        )

    def test_value(self):
        test_headers = {"key1": "value1", "key2": "value2"}
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            headers=test_headers,
        )
        self.assertEqual(mock_message(), d.value)
        self.assertEqual(test_headers, d.headers)

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


class TestReducerWindow(unittest.TestCase):
    def test_create_window(self):
        rw = ReduceWindow(start=mock_start_time(), end=mock_end_time(), slot="slot-0")
        i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
        self.assertEqual(rw.window, i)
        self.assertEqual(rw.start, mock_start_time())
        self.assertEqual(rw.end, mock_end_time())
        self.assertEqual(rw.slot, "slot-0")


class TestReducerClass(unittest.TestCase):
    class ExampleClass(Reducer):
        async def handler(
            self, keys: list[str], datums: AsyncIterable[Datum], md: Metadata
        ) -> Messages:
            pass

        def __init__(self, test1, test2):
            self.test1 = test1
            self.test2 = test2
            self.test3 = self.test1

    def test_init(self):
        r = self.ExampleClass(test1=1, test2=2)
        self.assertEqual(1, r.test1)
        self.assertEqual(2, r.test2)
        self.assertEqual(1, r.test3)

    def test_deep_copy(self):
        """Test that the deepcopy works as expected"""
        r = self.ExampleClass(test1=1, test2=2)
        # Create a copy of r
        r_copy = deepcopy(r)
        # Check that the attributes are the same
        self.assertEqual(1, r_copy.test1)
        self.assertEqual(2, r_copy.test2)
        self.assertEqual(1, r_copy.test3)
        # Check that the objects are not the same
        self.assertNotEqual(id(r), id(r_copy))
        # Update the attributes of r
        r.test1 = 5
        r.test3 = 6
        # Check that the other object is not updated
        self.assertNotEqual(r.test1, r_copy.test1)
        self.assertNotEqual(r.test3, r_copy.test3)
        self.assertNotEqual(id(r.test3), id(r_copy.test3))
        # Verify that the instance type is correct
        self.assertTrue(isinstance(r_copy, self.ExampleClass))
        self.assertTrue(isinstance(r_copy, Reducer))


if __name__ == "__main__":
    unittest.main()
