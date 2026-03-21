import asyncio
import pytest
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


# --- TestDatum ---


def test_datum_err_event_time():
    ts = _timestamp_pb2.Timestamp()
    ts.GetCurrentTime()
    headers = {"key1": "value1", "key2": "value2"}
    with pytest.raises(Exception) as exc_info:
        Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=ts,
            watermark=mock_watermark(),
            id_=TEST_ID,
            headers=headers,
        )
    assert (
        "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> " "for Datum.event_time"
    ) == str(exc_info.value)


def test_datum_err_watermark():
    ts = _timestamp_pb2.Timestamp()
    ts.GetCurrentTime()
    headers = {"key1": "value1", "key2": "value2"}
    with pytest.raises(Exception) as exc_info:
        Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=ts,
            id_=TEST_ID,
            headers=headers,
        )
    assert (
        "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> " "for Datum.watermark"
    ) == str(exc_info.value)


def test_datum_properties():
    d = Datum(
        keys=TEST_KEYS,
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        id_=TEST_ID,
        headers=TEST_HEADERS,
    )
    assert mock_message() == d.value
    assert TEST_KEYS == d.keys
    assert mock_event_time() == d.event_time
    assert mock_watermark() == d.watermark
    assert TEST_HEADERS == d.headers
    assert TEST_ID == d.id


def test_datum_default_values():
    d = Datum(
        keys=None,
        value=None,
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        id_=TEST_ID,
    )
    assert [] == d.keys
    assert b"" == d.value
    assert {} == d.headers


# --- TestIntervalWindow ---


def test_interval_window_start():
    i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
    assert mock_start_time() == i.start


def test_interval_window_end():
    i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
    assert mock_end_time() == i.end


# --- TestKeyedWindow ---


def test_keyed_window_create():
    kw = KeyedWindow(
        start=mock_start_time(), end=mock_end_time(), slot="slot-0", keys=["key1", "key2"]
    )
    assert kw.start == mock_start_time()
    assert kw.end == mock_end_time()
    assert kw.slot == "slot-0"
    assert kw.keys == ["key1", "key2"]


def test_keyed_window_default_values():
    kw = KeyedWindow(start=mock_start_time(), end=mock_end_time())
    assert kw.slot == ""
    assert kw.keys == []


def test_keyed_window_window_property():
    kw = KeyedWindow(start=mock_start_time(), end=mock_end_time())
    assert isinstance(kw.window, IntervalWindow)
    assert kw.window.start == mock_start_time()
    assert kw.window.end == mock_end_time()


# --- TestAccumulatorResult ---


def test_accumulator_result_create():
    # Create mock objects
    future = None  # In real usage, this would be an asyncio.Task
    iterator = NonBlockingIterator()
    keys = ["key1", "key2"]
    result_queue = NonBlockingIterator()
    consumer_future = None  # In real usage, this would be an asyncio.Task
    watermark = datetime.fromtimestamp(1662998400, timezone.utc)

    result = AccumulatorResult(future, iterator, keys, result_queue, consumer_future, watermark)

    assert result.future == future
    assert result.iterator == iterator
    assert result.keys == keys
    assert result.result_queue == result_queue
    assert result.consumer_future == consumer_future
    assert result.latest_watermark == watermark


def test_accumulator_result_update_watermark():
    result = AccumulatorResult(
        None, None, [], None, None, datetime.fromtimestamp(1662998400, timezone.utc)
    )
    new_watermark = datetime.fromtimestamp(1662998460, timezone.utc)
    result.update_watermark(new_watermark)
    assert result.latest_watermark == new_watermark


def test_accumulator_result_update_watermark_invalid_type():
    result = AccumulatorResult(
        None, None, [], None, None, datetime.fromtimestamp(1662998400, timezone.utc)
    )
    with pytest.raises(TypeError):
        result.update_watermark("not a datetime")


# --- TestAccumulatorRequest ---


def test_accumulator_request_create():
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
    assert request.operation == operation
    assert request.keyed_window == keyed_window
    assert request.payload == payload


# --- TestWindowOperation ---


def test_window_operation_enum_values():
    assert WindowOperation.OPEN == 0
    assert WindowOperation.CLOSE == 1
    assert WindowOperation.APPEND == 2


# --- TestMessage ---


def test_message_create():
    value = b"test_value"
    keys = ["key1", "key2"]
    tags = ["tag1", "tag2"]

    msg = Message(value=value, keys=keys, tags=tags)
    assert msg.value == value
    assert msg.keys == keys
    assert msg.tags == tags


def test_message_default_values():
    msg = Message(value=b"test")
    assert msg.keys == []
    assert msg.tags == []


def test_message_to_drop():
    msg = Message.to_drop()
    assert msg.value == b""
    assert msg.keys == []
    assert "U+005C__DROP__" in msg.tags


def test_message_none_values():
    msg = Message(value=None, keys=None, tags=None)
    assert msg.value == b""
    assert msg.keys == []
    assert msg.tags == []


def test_message_from_datum():
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
    assert message.value == test_value
    assert message.keys == test_keys
    assert message.event_time == test_event_time
    assert message.watermark == test_watermark
    assert message.headers == test_headers
    assert message.id == test_id

    # Verify that tags are empty (default for Message)
    assert message.tags == []


def test_message_from_datum_minimal():
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

    assert message.value == test_value
    assert message.keys == test_keys
    assert message.event_time == test_event_time
    assert message.watermark == test_watermark
    assert message.headers == {}
    assert message.id == test_id
    assert message.tags == []


def test_message_from_datum_empty_keys():
    """Test from_datum with empty keys"""
    datum = Datum(
        keys=None,  # Will default to []
        value=b"test_value",
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        id_="test_id",
    )

    message = Message.from_datum(datum)

    assert message.keys == []
    assert message.value == b"test_value"
    assert message.id == "test_id"


# --- TestAccumulatorClass ---


class _ExampleAccumulator(Accumulator):
    async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
        pass

    def __init__(self, test1, test2):
        self.test1 = test1
        self.test2 = test2
        self.test3 = self.test1


def test_accumulator_class_init():
    r = _ExampleAccumulator(test1=1, test2=2)
    assert 1 == r.test1
    assert 2 == r.test2
    assert 1 == r.test3


def test_accumulator_class_callable():
    """Test that accumulator instances can be called directly"""
    r = _ExampleAccumulator(test1=1, test2=2)
    # The __call__ method should be callable and delegate to the handler method
    assert callable(r)
    # __call__ should return the result of calling handler
    # Since handler is an async method, __call__ should return a coroutine

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
    assert asyncio.iscoroutine(result)
    # Clean up the coroutine
    result.close()
