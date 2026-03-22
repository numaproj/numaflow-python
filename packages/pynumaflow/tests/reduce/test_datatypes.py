from copy import deepcopy
from collections.abc import AsyncIterable

from pynumaflow.reducer import Reducer, Messages

from google.protobuf import timestamp_pb2 as _timestamp_pb2

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

import pytest

TEST_KEYS = ["test"]
TEST_ID = "test_id"


def test_datum_err_event_time():
    ts = _timestamp_pb2.Timestamp()
    ts.GetCurrentTime()
    headers = {"key1": "value1", "key2": "value2"}
    with pytest.raises(Exception) as exc_info:
        Datum(keys=TEST_KEYS, value=mock_message(), event_time=ts, watermark=ts, headers=headers)
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
            headers=headers,
        )
    assert (
        "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> " "for Datum.watermark"
    ) == str(exc_info.value)


def test_datum_value():
    test_headers = {"key1": "value1", "key2": "value2"}
    d = Datum(
        keys=TEST_KEYS,
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        headers=test_headers,
    )
    assert mock_message() == d.value
    assert test_headers == d.headers


def test_datum_key():
    d = Datum(
        keys=TEST_KEYS,
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
    )
    assert TEST_KEYS == d.keys


def test_datum_event_time():
    d = Datum(
        keys=TEST_KEYS,
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
    )
    assert mock_event_time() == d.event_time


def test_datum_watermark():
    d = Datum(
        keys=TEST_KEYS,
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
    )
    assert mock_watermark() == d.watermark


def test_interval_window_start():
    i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
    assert mock_start_time() == i.start


def test_interval_window_end():
    i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
    assert mock_end_time() == i.end


def test_metadata_interval_window():
    i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
    m = Metadata(interval_window=i)
    assert type(i) is type(m.interval_window)
    assert i == m.interval_window


def test_reduce_window_create():
    rw = ReduceWindow(start=mock_start_time(), end=mock_end_time(), slot="slot-0")
    i = IntervalWindow(start=mock_start_time(), end=mock_end_time())
    assert rw.window == i
    assert rw.start == mock_start_time()
    assert rw.end == mock_end_time()
    assert rw.slot == "slot-0"


class ExampleReducer(Reducer):
    async def handler(
        self, keys: list[str], datums: AsyncIterable[Datum], md: Metadata
    ) -> Messages:
        pass

    def __init__(self, test1, test2):
        self.test1 = test1
        self.test2 = test2
        self.test3 = self.test1


def test_reducer_class_init():
    r = ExampleReducer(test1=1, test2=2)
    assert 1 == r.test1
    assert 2 == r.test2
    assert 1 == r.test3


def test_reducer_class_deep_copy():
    """Test that the deepcopy works as expected"""
    r = ExampleReducer(test1=1, test2=2)
    # Create a copy of r
    r_copy = deepcopy(r)
    # Check that the attributes are the same
    assert 1 == r_copy.test1
    assert 2 == r_copy.test2
    assert 1 == r_copy.test3
    # Check that the objects are not the same
    assert id(r) != id(r_copy)
    # Update the attributes of r
    r.test1 = 5
    r.test3 = 6
    # Check that the other object is not updated
    assert r.test1 != r_copy.test1
    assert r.test3 != r_copy.test3
    assert id(r.test3) != id(r_copy.test3)
    # Verify that the instance type is correct
    assert isinstance(r_copy, ExampleReducer)
    assert isinstance(r_copy, Reducer)
