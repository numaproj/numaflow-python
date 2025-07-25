from copy import deepcopy
import unittest
from collections.abc import AsyncIterable
from datetime import datetime, timezone

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from pynumaflow.accumulator import Accumulator

from pynumaflow.accumulator._dtypes import (
    IntervalWindow,
    KeyedWindow,
    Metadata,
    Datum,
    AccumulatorResult,
    AccumulatorRequest,
    WindowOperation,
    Message,
    _AccumulatorBuilderClass,
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

    def test_value(self):
        test_headers = {"key1": "value1", "key2": "value2"}
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            id_=TEST_ID,
            headers=test_headers,
        )
        self.assertEqual(mock_message(), d.value)
        self.assertEqual(test_headers, d.headers)
        self.assertEqual(TEST_ID, d.id)

    def test_key(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            id_=TEST_ID,
        )
        self.assertEqual(TEST_KEYS, d.keys())

    def test_event_time(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            id_=TEST_ID,
        )
        self.assertEqual(mock_event_time(), d.event_time)

    def test_watermark(self):
        d = Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=mock_watermark(),
            id_=TEST_ID,
        )
        self.assertEqual(mock_watermark(), d.watermark)

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


class TestMetadata(unittest.TestCase):
    def test_interval_window(self):
        i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
        m = Metadata(interval_window=i)
        self.assertEqual(type(i), type(m.interval_window))
        self.assertEqual(i, m.interval_window)


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


class TestAccumulatorBuilderClass(unittest.TestCase):
    """Test AccumulatorBuilderClass functionality."""

    def test_builder_class_creation(self):
        """Test AccumulatorBuilderClass creation and instantiation."""

        class TestAccumulator(Accumulator):
            def __init__(self, counter=0):
                self.counter = counter

            async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
                pass

        builder = _AccumulatorBuilderClass(TestAccumulator, (15,), {})
        instance = builder.create()

        self.assertIsInstance(instance, TestAccumulator)
        self.assertEqual(instance.counter, 15)

    def test_builder_class_with_kwargs(self):
        """Test AccumulatorBuilderClass with keyword arguments."""

        class KwargsAccumulator(Accumulator):
            def __init__(self, param1, param2, param3=None):
                self.param1 = param1
                self.param2 = param2
                self.param3 = param3

            async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
                pass

        builder = _AccumulatorBuilderClass(
            KwargsAccumulator, ("arg1", "arg2"), {"param3": "kwarg_value"}
        )
        instance = builder.create()

        self.assertIsInstance(instance, KwargsAccumulator)
        self.assertEqual(instance.param1, "arg1")
        self.assertEqual(instance.param2, "arg2")
        self.assertEqual(instance.param3, "kwarg_value")

    def test_builder_class_empty_args(self):
        """Test AccumulatorBuilderClass with empty args and kwargs."""

        class EmptyArgsAccumulator(Accumulator):
            def __init__(self):
                self.initialized = True

            async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
                pass

        builder = _AccumulatorBuilderClass(EmptyArgsAccumulator, (), {})
        instance = builder.create()

        self.assertIsInstance(instance, EmptyArgsAccumulator)
        self.assertTrue(instance.initialized)


class TestAsyncServerHandlerCoverage(unittest.TestCase):
    """Test async server handler function coverage."""

    def test_get_handler_with_function_and_args_error(self):
        """Test get_handler raises TypeError when function handler is passed with init args."""
        from pynumaflow.accumulator.async_server import get_handler

        async def test_func(datums, output):
            pass

        with self.assertRaises(TypeError) as context:
            get_handler(test_func, init_args=(1, 2))

        self.assertIn(
            "Cannot pass function handler with init args or kwargs", str(context.exception)
        )

    def test_get_handler_with_function_and_kwargs_error(self):
        """Test get_handler raises TypeError when function handler is passed with init kwargs."""
        from pynumaflow.accumulator.async_server import get_handler

        async def test_func(datums, output):
            pass

        with self.assertRaises(TypeError) as context:
            get_handler(test_func, init_kwargs={"test": "value"})

        self.assertIn(
            "Cannot pass function handler with init args or kwargs", str(context.exception)
        )

    def test_get_handler_with_invalid_class(self):
        """Test get_handler raises TypeError for invalid class type."""
        from pynumaflow.accumulator.async_server import get_handler

        class InvalidClass:
            pass

        with self.assertRaises(TypeError) as context:
            get_handler(InvalidClass())

        # The actual error comes from issubclass() check since we're passing an instance
        self.assertIn("issubclass() arg 1 must be a class", str(context.exception))

    def test_get_handler_with_invalid_class_type(self):
        """Test get_handler raises TypeError for invalid Accumulator class type."""
        from pynumaflow.accumulator.async_server import get_handler

        class NonAccumulatorClass:
            pass

        with self.assertRaises(TypeError) as context:
            get_handler(NonAccumulatorClass)

        # This will hit the 'Invalid Class Type' error path
        self.assertIn("Invalid Class Type", str(context.exception))

    def test_get_handler_with_valid_class(self):
        """Test get_handler returns AccumulatorBuilderClass for valid Accumulator subclass."""
        from pynumaflow.accumulator.async_server import get_handler

        class ValidAccumulator(Accumulator):
            def __init__(self, counter=0):
                self.counter = counter

            async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
                pass

        result = get_handler(ValidAccumulator, init_args=(10,), init_kwargs={"counter": 5})

        self.assertIsInstance(result, _AccumulatorBuilderClass)


class TestTaskManagerUtilities(unittest.TestCase):
    """Test TaskManager utility functions."""

    def test_build_window_hash(self):
        """Test build_window_hash function."""
        from pynumaflow.accumulator.servicer.task_manager import build_window_hash
        from unittest.mock import Mock

        # Create a mock window with ToMilliseconds method
        mock_window = Mock()
        mock_window.start.ToMilliseconds.return_value = 1000
        mock_window.end.ToMilliseconds.return_value = 2000

        result = build_window_hash(mock_window)
        self.assertEqual(result, "1000:2000")

    def test_build_unique_key_name(self):
        """Test build_unique_key_name function."""
        from pynumaflow.accumulator.servicer.task_manager import build_unique_key_name

        keys = ["key1", "key2", "key3"]
        result = build_unique_key_name(keys)

        self.assertEqual(result, "key1:key2:key3")

    def test_task_manager_initialization(self):
        """Test TaskManager initialization."""
        import asyncio
        from pynumaflow.accumulator.servicer.task_manager import TaskManager
        from unittest.mock import Mock

        async def run_test():
            handler = Mock()
            task_manager = TaskManager(handler)

            self.assertEqual(task_manager._TaskManager__accumulator_handler, handler)
            self.assertEqual(len(task_manager.tasks), 0)
            self.assertEqual(len(task_manager.background_tasks), 0)

        asyncio.run(run_test())

    def test_task_manager_get_unique_windows(self):
        """Test TaskManager get_unique_windows with empty tasks."""
        import asyncio
        from pynumaflow.accumulator.servicer.task_manager import TaskManager
        from unittest.mock import Mock

        async def run_test():
            handler = Mock()
            task_manager = TaskManager(handler)

            windows = task_manager.get_unique_windows()
            self.assertEqual(len(windows), 0)

        asyncio.run(run_test())

    def test_task_manager_get_tasks(self):
        """Test TaskManager get_tasks method."""
        import asyncio
        from pynumaflow.accumulator.servicer.task_manager import TaskManager
        from unittest.mock import Mock

        async def run_test():
            handler = Mock()
            task_manager = TaskManager(handler)

            tasks = task_manager.get_tasks()
            self.assertEqual(len(tasks), 0)

        asyncio.run(run_test())

    def test_task_manager_close_task_not_found(self):
        """Test TaskManager close_task when task is not found."""
        import asyncio
        from unittest.mock import patch, Mock
        from pynumaflow.accumulator.servicer.task_manager import TaskManager

        with patch("pynumaflow.accumulator.servicer.task_manager._LOGGER") as mock_logger:
            handler = Mock()
            task_manager = TaskManager(handler)

            # Create a mock request with payload that has keys
            mock_request = Mock()
            mock_datum = Mock()
            mock_datum.keys.return_value = ["test_key"]
            mock_request.payload = mock_datum

            # Call close_task - should log error and put exception in queue
            asyncio.run(task_manager.close_task(mock_request))

            # Verify logger was called
            mock_logger.critical.assert_called_once_with(
                "accumulator task not found", exc_info=True
            )


class TestAsyncServicerCoverage(unittest.TestCase):
    """Test AsyncAccumulatorServicer coverage."""

    def test_servicer_is_ready_method(self):
        """Test AsyncAccumulatorServicer IsReady method."""
        import asyncio
        from unittest.mock import Mock
        from pynumaflow.accumulator.servicer.async_servicer import AsyncAccumulatorServicer
        from pynumaflow.proto.accumulator import accumulator_pb2
        from google.protobuf import empty_pb2 as _empty_pb2

        async def run_test():
            mock_handler = Mock()
            servicer = AsyncAccumulatorServicer(mock_handler)

            # Create mock context and empty request
            mock_context = Mock()
            empty_request = _empty_pb2.Empty()

            # Call IsReady
            response = await servicer.IsReady(empty_request, mock_context)

            # Verify response
            self.assertIsInstance(response, accumulator_pb2.ReadyResponse)
            self.assertTrue(response.ready)

        asyncio.run(run_test())

    def test_datum_generator(self):
        """Test datum_generator function."""
        import asyncio
        from unittest.mock import Mock
        from pynumaflow.accumulator.servicer.async_servicer import datum_generator
        from tests.testing_utils import (
            get_time_args,
            mock_interval_window_start,
            mock_interval_window_end,
        )

        async def run_test():
            # Create mock request
            event_time_timestamp, watermark_timestamp = get_time_args()

            mock_request = Mock()
            mock_request.operation.event = WindowOperation.OPEN.value
            mock_request.operation.keyedWindow.start.ToDatetime.return_value = (
                mock_interval_window_start().ToDatetime()
            )
            mock_request.operation.keyedWindow.end.ToDatetime.return_value = (
                mock_interval_window_end().ToDatetime()
            )
            mock_request.operation.keyedWindow.slot = "slot-0"
            mock_request.operation.keyedWindow.keys = ["test_key"]

            mock_request.payload.keys = ["test_key"]
            mock_request.payload.value = b"test_value"
            mock_request.payload.event_time.ToDatetime.return_value = (
                event_time_timestamp.ToDatetime()
            )
            mock_request.payload.watermark.ToDatetime.return_value = (
                watermark_timestamp.ToDatetime()
            )
            mock_request.payload.id = "test_id"
            mock_request.payload.headers = {"header1": "value1"}

            async def mock_request_iterator():
                yield mock_request

            results = []
            async for result in datum_generator(mock_request_iterator()):
                results.append(result)

            self.assertEqual(len(results), 1)
            self.assertIsInstance(results[0], AccumulatorRequest)
            self.assertEqual(results[0].operation, WindowOperation.OPEN.value)

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
