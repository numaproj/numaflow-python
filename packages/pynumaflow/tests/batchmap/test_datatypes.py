import pytest

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


def test_datum_err_event_time():
    ts = _timestamp_pb2.Timestamp()
    ts.GetCurrentTime()
    with pytest.raises(Exception) as exc_info:
        Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=ts,
            watermark=ts,
            headers=TEST_HEADERS,
            id=TEST_ID,
        )
    assert (
        "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> " "for Datum.event_time"
    ) == str(exc_info.value)


def test_datum_err_watermark():
    ts = _timestamp_pb2.Timestamp()
    ts.GetCurrentTime()
    with pytest.raises(Exception) as exc_info:
        Datum(
            keys=TEST_KEYS,
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=ts,
            headers=TEST_HEADERS,
            id=TEST_ID,
        )
    assert (
        "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> " "for Datum.watermark"
    ) == str(exc_info.value)


def test_datum_value():
    d = Datum(
        keys=TEST_KEYS,
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        headers=TEST_HEADERS,
        id=TEST_ID,
    )
    assert mock_message() == d.value


def test_datum_key():
    d = Datum(
        keys=TEST_KEYS,
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        id=TEST_ID,
    )
    assert TEST_KEYS == d.keys


def test_datum_event_time():
    d = Datum(
        keys=TEST_KEYS,
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        headers=TEST_HEADERS,
        id=TEST_ID,
    )
    assert mock_event_time() == d.event_time
    assert TEST_HEADERS == d.headers


def test_datum_watermark():
    d = Datum(
        keys=TEST_KEYS,
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        id=TEST_ID,
    )
    assert mock_watermark() == d.watermark
    assert {} == d.headers


def test_datum_id():
    d = Datum(
        keys=TEST_KEYS,
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        id=TEST_ID,
    )
    assert TEST_ID == d.id
    assert {} == d.headers
