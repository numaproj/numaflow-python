import pytest

from pynumaflow.mapstreamer import Messages, Message, DROP
from tests.testing_utils import mock_message


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
