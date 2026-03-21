import pytest

from pynumaflow.sourcer import (
    Message,
    Offset,
    ReadRequest,
    PartitionsResponse,
)
from tests.source.utils import mock_offset
from tests.testing_utils import mock_event_time


def test_message_creation():
    payload = b"payload:test_mock_message"
    keys = ["test_key"]
    offset = mock_offset()
    event_time = mock_event_time()
    headers = {"key1": "value1", "key2": "value2"}
    msg = Message(payload=payload, offset=offset, keys=keys, event_time=event_time, headers=headers)
    assert event_time == msg.event_time
    assert payload == msg.payload
    assert keys == msg.keys
    assert offset == msg.offset
    assert headers == msg.headers


def test_offset_creation():
    msg = Offset(offset=mock_offset().offset, partition_id=mock_offset().partition_id)
    assert msg.offset == mock_offset().offset
    assert msg.partition_id == mock_offset().partition_id


def test_default_offset_creation():
    msg = Offset.offset_with_default_partition_id(mock_offset().offset)
    assert msg.offset == mock_offset().offset
    assert msg.partition_id == 0


def test_datum_creation():
    msg = ReadRequest(num_records=1, timeout_in_ms=1000)
    assert msg.num_records == 1
    assert msg.timeout_in_ms == 1000


def test_err_num_record():
    with pytest.raises(TypeError, match="Wrong data type"):
        ReadRequest(num_records="HEKKO", timeout_in_ms=1000)


def test_err_timeout():
    with pytest.raises(TypeError, match="Wrong data type"):
        ReadRequest(num_records=1, timeout_in_ms="1000")


def test_partition_response():
    msg = PartitionsResponse(partitions=[1, 2, 3])
    assert msg.partitions == [1, 2, 3]


def test_err_partition():
    with pytest.raises(TypeError, match="Wrong data type"):
        PartitionsResponse(partitions="HEKKO")
