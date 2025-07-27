from copy import deepcopy
import unittest
from collections.abc import AsyncIterable
from datetime import datetime, timezone

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from pynumaflow.accumulator import Accumulator

from pynumaflow.accumulator._dtypes import (
    IntervalWindow,
    KeyedWindow,
    Datum,
    AccumulatorResult,
    AccumulatorRequest,
    WindowOperation,
    Message,
)
from pynumaflow.shared.asynciter import NonBlockingIterator
from tests.testing_utils import (
    mock_message,
    mock_event_time,
    mock_watermark,
    mock_start_time,
    mock_end_time,
)

TEST_KEYS = ["test"]
TEST_ID = "test_id"
TEST_HEADERS = {"key1": "value1", "key2": "value2"}


class TestDatum(unittest.TestCase):
    def test_err_event_time(self):
        ts = _timestamp_pb2.Timestamp()
        ts.GetCurrentTime()
        headers = {"key1": "value1", "key2": "value2"}
        with self.assertRaises(Exception) as context:
            Datum(
                keys=TEST_KEYS,
                value=mock_message(),
                event_time=ts,
                watermark=mock_watermark(),
                id_=TEST_ID,
                headers=headers,
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
                id_=TEST_ID,
                headers=headers,
            )
        self.assertEqual(
            "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> "
            "for Datum.watermark",
            str(context.exception),
        )

    def test_properties(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            id_=TEST_ID,
            headers=TEST_HEADERS,
        )
        self.assertEqual(mock_message(), d.value)
        self.assertEqual(TEST_KEYS, d.keys())
        self.assertEqual(mock_event_time(), d.event_time)
        self.assertEqual(mock_watermark(), d.watermark)
        self.assertEqual(TEST_HEADERS, d.headers)
        self.assertEqual(TEST_ID, d.id)

    def test_default_values(self):
        d = Datum(
            keys=None,
            value=None,
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            id_=TEST_ID,
        )
        self.assertEqual([], d.keys())
        self.assertEqual(b"", d.value)
        self.assertEqual({}, d.headers)


class TestIntervalWindow(unittest.TestCase):
    def test_start(self):
        i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
        self.assertEqual(mock_start_time(), i.start)

    def test_end(self):
        i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
        self.assertEqual(mock_end_time(), i.end)


class TestKeyedWindow(unittest.TestCase):
    def test_create_window(self):
        kw = KeyedWindow(
            start=mock_start_time(), end=mock_end_time(), slot="slot-0", keys=["key1", "key2"]
        )
        self.assertEqual(kw.start, mock_start_time())
        self.assertEqual(kw.end, mock_end_time())
        self.assertEqual(kw.slot, "slot-0")
        self.assertEqual(kw.keys, ["key1", "key2"])

    def test_default_values(self):
        kw = KeyedWindow(start=mock_start_time(), end=mock_end_time())
        self.assertEqual(kw.slot, "")
        self.assertEqual(kw.keys, [])

    def test_window_property(self):
        kw = KeyedWindow(start=mock_start_time(), end=mock_end_time())
        self.assertIsInstance(kw.window, IntervalWindow)
        self.assertEqual(kw.window.start, mock_start_time())
        self.assertEqual(kw.window.end, mock_end_time())


class TestAccumulatorResult(unittest.TestCase):
    def test_create_result(self):
        # Create mock objects
        future = None  # In real usage, this would be an asyncio.Task
        iterator = NonBlockingIterator()
        keys = ["key1", "key2"]
        result_queue = NonBlockingIterator()
        consumer_future = None  # In real usage, this would be an asyncio.Task
        watermark = datetime.fromtimestamp(1662998400, timezone.utc)

        result = AccumulatorResult(future, iterator, keys, result_queue, consumer_future, watermark)

        self.assertEqual(result.future, future)
        self.assertEqual(result.iterator, iterator)
        self.assertEqual(result.keys, keys)
        self.assertEqual(result.result_queue, result_queue)
        self.assertEqual(result.consumer_future, consumer_future)
        self.assertEqual(result.latest_watermark, watermark)

    def test_update_watermark(self):
        result = AccumulatorResult(
            None, None, [], None, None, datetime.fromtimestamp(1662998400, timezone.utc)
        )
        new_watermark = datetime.fromtimestamp(1662998460, timezone.utc)
        result.update_watermark(new_watermark)
        self.assertEqual(result.latest_watermark, new_watermark)

    def test_update_watermark_invalid_type(self):
        result = AccumulatorResult(
            None, None, [], None, None, datetime.fromtimestamp(1662998400, timezone.utc)
        )
        with self.assertRaises(TypeError):
            result.update_watermark("not a datetime")


class TestAccumulatorRequest(unittest.TestCase):
    def test_create_request(self):
        operation = WindowOperation.OPEN
        keyed_window = KeyedWindow(start=mock_start_time(), end=mock_end_time())
        payload = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            id_=TEST_ID,
        )

        request = AccumulatorRequest(operation, keyed_window, payload)
        self.assertEqual(request.operation, operation)
        self.assertEqual(request.keyed_window, keyed_window)
        self.assertEqual(request.payload, payload)


class TestWindowOperation(unittest.TestCase):
    def test_enum_values(self):
        self.assertEqual(WindowOperation.OPEN, 0)
        self.assertEqual(WindowOperation.CLOSE, 1)
        self.assertEqual(WindowOperation.APPEND, 2)


class TestMessage(unittest.TestCase):
    def test_create_message(self):
        value = b"test_value"
        keys = ["key1", "key2"]
        tags = ["tag1", "tag2"]

        msg = Message(value=value, keys=keys, tags=tags)
        self.assertEqual(msg.value, value)
        self.assertEqual(msg.keys, keys)
        self.assertEqual(msg.tags, tags)

    def test_default_values(self):
        msg = Message(value=b"test")
        self.assertEqual(msg.keys, [])
        self.assertEqual(msg.tags, [])

    def test_to_drop(self):
        msg = Message.to_drop()
        self.assertEqual(msg.value, b"")
        self.assertEqual(msg.keys, [])
        self.assertTrue("U+005C__DROP__" in msg.tags)

    def test_none_values(self):
        msg = Message(value=None, keys=None, tags=None)
        self.assertEqual(msg.value, b"")
        self.assertEqual(msg.keys, [])
        self.assertEqual(msg.tags, [])

    def test_from_datum(self):
        """Test that Message.from_datum correctly creates a Message from a Datum"""
        # Create a sample datum with all properties
        test_keys = ["key1", "key2"]
        test_value = b"test_message_value"
        test_event_time = mock_event_time()
        test_watermark = mock_watermark()
        test_headers = {"header1": "value1", "header2": "value2"}
        test_id = "test_datum_id"

        datum = Datum(
            keys=test_keys,
            value=test_value,
            event_time=test_event_time,
            watermark=test_watermark,
            id_=test_id,
            headers=test_headers,
        )

        # Create message from datum
        message = Message.from_datum(datum)

        # Verify all properties are correctly transferred
        self.assertEqual(message.value, test_value)
        self.assertEqual(message.keys, test_keys)
        self.assertEqual(message.event_time, test_event_time)
        self.assertEqual(message.watermark, test_watermark)
        self.assertEqual(message.headers, test_headers)
        self.assertEqual(message.id, test_id)

        # Verify that tags are empty (default for Message)
        self.assertEqual(message.tags, [])

    def test_from_datum_minimal(self):
        """Test from_datum with minimal Datum (no headers)"""
        test_keys = ["minimal_key"]
        test_value = b"minimal_value"
        test_event_time = mock_event_time()
        test_watermark = mock_watermark()
        test_id = "minimal_id"

        datum = Datum(
            keys=test_keys,
            value=test_value,
            event_time=test_event_time,
            watermark=test_watermark,
            id_=test_id,
            # headers not provided (will default to {})
        )

        message = Message.from_datum(datum)

        self.assertEqual(message.value, test_value)
        self.assertEqual(message.keys, test_keys)
        self.assertEqual(message.event_time, test_event_time)
        self.assertEqual(message.watermark, test_watermark)
        self.assertEqual(message.headers, {})
        self.assertEqual(message.id, test_id)
        self.assertEqual(message.tags, [])

    def test_from_datum_empty_keys(self):
        """Test from_datum with empty keys"""
        datum = Datum(
            keys=None,  # Will default to []
            value=b"test_value",
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            id_="test_id",
        )

        message = Message.from_datum(datum)

        self.assertEqual(message.keys, [])
        self.assertEqual(message.value, b"test_value")
        self.assertEqual(message.id, "test_id")


class TestAccumulatorClass(unittest.TestCase):
    class ExampleClass(Accumulator):
        async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
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
        self.assertTrue(isinstance(r_copy, Accumulator))

    def test_callable(self):
        """Test that accumulator instances can be called directly"""
        r = self.ExampleClass(test1=1, test2=2)
        # The __call__ method should be callable and delegate to the handler method
        self.assertTrue(callable(r))
        # __call__ should return the result of calling handler
        # Since handler is an async method, __call__ should return a coroutine
        import asyncio
        from pynumaflow.shared.asynciter import NonBlockingIterator

        async def test_datums():
            yield Datum(
                keys=["test"],
                value=b"test",
                event_time=mock_event_time(),
                watermark=mock_watermark(),
                id_="test",
            )

        output = NonBlockingIterator()
        result = r(test_datums(), output)
        self.assertTrue(asyncio.iscoroutine(result))
        # Clean up the coroutine
        result.close()


if __name__ == "__main__":
    unittest.main()
