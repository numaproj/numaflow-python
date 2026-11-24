import pytest

from pynumaflow.mapstreamer import (
    Messages,
    Message,
    Datum,
    DROP,
    NackOptions,
    UserMetadata,
    SystemMetadata,
)
from pynumaflow._constants import FAIL, NACK
from tests.testing_utils import mock_message, mock_event_time, mock_watermark


def test_message_to_nack():
    opts = NackOptions(max_deliveries=2)
    msg = Message.to_nack(opts)
    assert type(msg) is Message
    assert msg.keys == []
    assert msg.value == b""
    assert msg.tags == [NACK]
    assert msg.nack_options == opts


def test_message_to_nack_default_opts():
    msg = Message.to_nack()
    assert msg.tags == [NACK]
    assert msg.nack_options is None


def test_message_default_nack_options():
    msg = Message(mock_message())
    assert msg.nack_options is None


def test_message_default_user_metadata():
    msg = Message(mock_message())
    assert msg.user_metadata == UserMetadata()
    assert len(msg.user_metadata) == 0


def test_message_user_metadata():
    user_metadata = UserMetadata()
    user_metadata.add_key("group", "key", b"value")
    msg = Message(mock_message(), user_metadata=user_metadata)
    assert msg.user_metadata.value("group", "key") == b"value"
    assert msg.user_metadata._to_proto().user_metadata["group"].key_value == {"key": b"value"}


def test_datum_default_metadata():
    d = Datum(
        keys=["test"],
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
    )
    assert d.user_metadata == UserMetadata()
    assert d.system_metadata == SystemMetadata()


def test_datum_metadata():
    user_metadata = UserMetadata({"app": {"k": b"v"}})
    system_metadata = SystemMetadata({"tracing": {"traceparent": b"00-trace-span-01"}})
    d = Datum(
        keys=["test"],
        value=mock_message(),
        event_time=mock_event_time(),
        watermark=mock_watermark(),
        user_metadata=user_metadata,
        system_metadata=system_metadata,
    )
    assert d.user_metadata.value("app", "k") == b"v"
    assert d.system_metadata.groups() == ["tracing"]
    assert d.system_metadata.value("tracing", "traceparent") == b"00-trace-span-01"


def test_message_to_fail():
    msg = Message.to_fail()
    assert type(msg) is Message
    assert msg.keys == []
    assert msg.value == b""
    assert msg.tags == [FAIL]


def test_message_key():
    mock_obj = {"Keys": ["test-key"], "Value": mock_message()}
    msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"])
    print(msg)
    assert mock_obj["Keys"] == msg.keys


def test_message_value():
    mock_obj = {"Keys": ["test-key"], "Value": mock_message()}
    msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"])
    assert mock_obj["Value"] == msg.value


def test_message_to_all():
    mock_obj = {"Keys": [], "Value": mock_message(), "Tags": []}
    msg = Message(mock_obj["Value"])
    assert type(msg) is Message
    assert mock_obj["Keys"] == msg.keys
    assert mock_obj["Value"] == msg.value
    assert mock_obj["Tags"] == msg.tags


def test_message_to_drop():
    mock_obj = {"Keys": [], "Value": b"", "Tags": [DROP]}
    msg = Message(b"").to_drop()
    assert type(msg) is Message
    assert mock_obj["Keys"] == msg.keys
    assert mock_obj["Value"] == msg.value
    assert mock_obj["Tags"] == msg.tags


def test_message_to():
    mock_obj = {"Keys": ["__KEY__"], "Value": mock_message(), "Tags": ["__TAG__"]}
    msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"], tags=mock_obj["Tags"])
    assert type(msg) is Message
    assert mock_obj["Keys"] == msg.keys
    assert mock_obj["Value"] == msg.value
    assert mock_obj["Tags"] == msg.tags


def _mock_message_object():
    value = mock_message()
    return Message(value=value)


def test_messages_items():
    mock_obj = [
        {"Keys": ["test_key"], "Value": mock_message()},
        {"Keys": ["test_key"], "Value": mock_message()},
    ]
    msgs = Messages(*mock_obj)
    assert len(mock_obj) == len(msgs)
    assert len(mock_obj) == len(msgs.items())
    assert mock_obj[0]["Keys"] == msgs[0]["Keys"]
    assert mock_obj[0]["Value"] == msgs[0]["Value"]
    assert (
        "[{'Keys': ['test_key'], 'Value': b'test_mock_message'}, "
        "{'Keys': ['test_key'], 'Value': b'test_mock_message'}]"
    ) == repr(msgs)


def test_messages_append():
    msgs = Messages()
    assert 0 == len(msgs)
    msgs.append(_mock_message_object())
    assert 1 == len(msgs)
    msgs.append(_mock_message_object())
    assert 2 == len(msgs)


def test_messages_forward_to_drop():
    mock_obj = Messages()
    mock_obj.append(Message(b"").to_drop())
    true_obj = Messages()
    true_obj.append(mock_obj[0])
    assert type(mock_obj) is type(true_obj)
    for i in range(len(true_obj)):
        assert type(mock_obj[i]) is type(true_obj[i])
        assert mock_obj[i].keys == true_obj[i].keys
        assert mock_obj[i].value == true_obj[i].value
    for msg in true_obj:
        print(msg)


def test_messages_err():
    msgts = Messages(_mock_message_object(), _mock_message_object())
    with pytest.raises(TypeError):
        msgts[:1]
