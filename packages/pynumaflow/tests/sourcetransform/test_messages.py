import pytest
from datetime import datetime, timezone

from pynumaflow.sourcetransformer import (
    Messages,
    Message,
    DROP,
    SourceTransformer,
    Datum,
    UserMetadata,
    SystemMetadata,
)
from tests.testing_utils import mock_new_event_time


def mock_message_t():
    msgt = bytes("test_mock_message_t", encoding="utf-8")
    return msgt


def mock_event_time():
    t = datetime.fromtimestamp(1662998400, timezone.utc)
    return t


def _mock_message_object():
    value = mock_message_t()
    event_time = mock_event_time()
    return Message(value=value, event_time=event_time)


# --- TestMessage ---


def test_message_creation():
    mock_obj = {
        "Keys": ["test_key"],
        "Value": mock_message_t(),
        "EventTime": mock_event_time(),
        "Tags": ["test_tag"],
    }
    msgt = Message(
        mock_obj["Value"], mock_obj["EventTime"], keys=mock_obj["Keys"], tags=mock_obj["Tags"]
    )
    assert mock_obj["EventTime"] == msgt.event_time
    assert mock_obj["Value"] == msgt.value
    assert mock_obj["Keys"] == msgt.keys
    assert mock_obj["Tags"] == msgt.tags


def test_message_to_drop():
    mock_obj = {
        "Keys": [],
        "Value": b"",
        "Tags": [DROP],
        "EventTime": mock_event_time(),
    }
    msgt = Message(b"", datetime(1, 1, 1, 0, 0)).to_drop(mock_event_time())
    assert isinstance(msgt, Message)
    assert mock_obj["Keys"] == msgt.keys
    assert mock_obj["Value"] == msgt.value
    assert mock_obj["Tags"] == msgt.tags
    assert mock_obj["EventTime"] == msgt.event_time


def test_message_with_user_metadata():
    user_meta = UserMetadata()
    user_meta.add_key("group1", "key1", b"value1")
    user_meta.add_key("group1", "key2", b"value2")

    msgt = Message(
        mock_message_t(),
        mock_event_time(),
        keys=["test_key"],
        user_metadata=user_meta,
    )
    assert mock_message_t() == msgt.value
    assert ["test_key"] == msgt.keys
    assert b"value1" == msgt.user_metadata.value("group1", "key1")
    assert b"value2" == msgt.user_metadata.value("group1", "key2")
    assert ["group1"] == msgt.user_metadata.groups()


def test_message_default_user_metadata():
    msgt = Message(mock_message_t(), mock_event_time())
    assert msgt.user_metadata is not None
    assert 0 == len(msgt.user_metadata)


# --- TestMessages ---


def test_messages_items():
    mock_obj = [
        {
            "Keys": [b"U+005C__ALL__"],
            "Value": mock_message_t(),
            "EventTime": mock_event_time(),
        },
        {
            "Keys": [b"U+005C__ALL__"],
            "Value": mock_message_t(),
            "EventTime": mock_event_time(),
        },
    ]
    msgts = Messages(*mock_obj)
    assert len(mock_obj) == len(msgts)
    assert len(mock_obj) == len(msgts.items())
    assert mock_obj[0]["Keys"] == msgts[0]["Keys"]
    assert mock_obj[0]["Value"] == msgts[0]["Value"]
    assert mock_obj[0]["EventTime"] == msgts[0]["EventTime"]
    assert (
        "[{'Keys': [b'U+005C__ALL__'], 'Value': b'test_mock_message_t', "
        "'EventTime': datetime.datetime(2022, 9, 12, 16, 0, tzinfo=datetime.timezone.utc)}, "
        "{'Keys': [b'U+005C__ALL__'], 'Value': b'test_mock_message_t', "
        "'EventTime': datetime.datetime(2022, 9, 12, 16, 0, tzinfo=datetime.timezone.utc)}]"
    ) == repr(msgts)


def test_messages_append():
    msgts = Messages()
    assert 0 == len(msgts)
    msgts.append(_mock_message_object())
    assert 1 == len(msgts)
    msgts.append(_mock_message_object())
    assert 2 == len(msgts)


def test_messages_err():
    msgts = Messages(_mock_message_object(), _mock_message_object())
    with pytest.raises(TypeError):
        msgts[:1]


# --- TestDatum ---


def test_datum_with_metadata():
    user_meta = UserMetadata()
    user_meta.add_key("group1", "key1", b"value1")

    sys_meta = SystemMetadata({"sys_group": {"sys_key": b"sys_value"}})

    d = Datum(
        keys=["test_key"],
        value=mock_message_t(),
        event_time=mock_event_time(),
        watermark=mock_event_time(),
        headers={"header1": "value1"},
        user_metadata=user_meta,
        system_metadata=sys_meta,
    )
    assert ["test_key"] == d.keys
    assert mock_message_t() == d.value
    assert mock_event_time() == d.event_time
    assert {"header1": "value1"} == d.headers
    assert b"value1" == d.user_metadata.value("group1", "key1")
    assert b"sys_value" == d.system_metadata.value("sys_group", "sys_key")


def test_datum_default_metadata():
    d = Datum(
        keys=["test_key"],
        value=mock_message_t(),
        event_time=mock_event_time(),
        watermark=mock_event_time(),
    )
    assert d.user_metadata is not None
    assert d.system_metadata is not None
    assert 0 == len(d.user_metadata)
    assert [] == d.system_metadata.groups()


# --- TestSourceTransformClass ---


class ExampleSourceTransformClass(SourceTransformer):
    def handler(self, keys: list[str], datum: Datum) -> Messages:
        messages = Messages()
        messages.append(Message(mock_message_t(), mock_new_event_time(), keys=keys))
        return messages


def test_source_transform_class_call():
    """Test that the __call__ functionality for the class works,
    ie the class instance can be called directly to invoke the handler function
    """
    transform_instance = ExampleSourceTransformClass()
    # make a call to the class directly
    ret = transform_instance([], None)
    assert mock_message_t() == ret[0].value
    # make a call to the handler
    ret_handler = transform_instance.handler([], None)
    # Both responses should be equal
    assert ret[0] == ret_handler[0]
