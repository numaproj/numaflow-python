from datetime import datetime, timezone

import pytest
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow.sinker._dtypes import (
    Datum,
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


def mock_headers():
    headers = {"key1": "value1", "key2": "value2"}
    return headers


def test_err_event_time():
    ts = _timestamp_pb2.Timestamp()
    ts.GetCurrentTime()
    with pytest.raises(Exception) as exc_info:
        Datum(
            keys=["test_key"],
            sink_msg_id="test_id_0",
            value=mock_message(),
            event_time=ts,
            watermark=ts,
            headers=mock_headers(),
        )
    assert (
        "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> "
        "for Datum.event_time" == str(exc_info.value)
    )


def test_err_watermark():
    ts = _timestamp_pb2.Timestamp()
    ts.GetCurrentTime()
    with pytest.raises(Exception) as exc_info:
        Datum(
            keys=["test_key"],
            sink_msg_id="test_id_0",
            value=mock_message(),
            event_time=mock_event_time(),
            watermark=ts,
            headers=mock_headers(),
        )
    assert (
        "Wrong data type: <class 'google.protobuf.timestamp_pb2.Timestamp'> "
        "for Datum.watermark" == str(exc_info.value)
    )


def test_value():
    d = Datum(
        keys=["test_key"],
        sink_msg_id="test_id_0",
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        headers=mock_headers(),
    )
    assert mock_message() == d.value
    assert (
        "keys: ['test_key'], "
        "id: test_id_0, value: test_mock_message, "
        "event_time: 2022-09-12 16:00:00+00:00, watermark: 2022-09-12 16:01:00+00:00, "
        "headers: {'key1': 'value1', 'key2': 'value2'}" == str(d)
    )
    assert (
        "keys: ['test_key'], "
        "id: test_id_0, value: test_mock_message, "
        "event_time: 2022-09-12 16:00:00+00:00, "
        "watermark: 2022-09-12 16:01:00+00:00, "
        "headers: {'key1': 'value1', 'key2': 'value2'}" == repr(d)
    )
    assert mock_headers() == d.headers


def test_id():
    d = Datum(
        keys=["test_key"],
        sink_msg_id="test_id_0",
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        headers=mock_headers(),
    )
    assert "test_id_0" == d.id


def test_event_time():
    d = Datum(
        keys=["test_key"],
        sink_msg_id="test_id_0",
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        headers=mock_headers(),
    )
    assert mock_event_time() == d.event_time


def test_watermark():
    d = Datum(
        keys=["test_key"],
        sink_msg_id="test_id_0",
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        headers=mock_headers(),
    )
    assert mock_watermark() == d.watermark
