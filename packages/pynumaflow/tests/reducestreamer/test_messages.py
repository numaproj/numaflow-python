from pynumaflow.reducestreamer import Message, DROP
from tests.testing_utils import mock_message


def test_key():
    mock_obj = {"Keys": ["test-key"], "Value": mock_message()}
    msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"])
    print(msg)
    assert mock_obj["Keys"] == msg.keys


def test_value():
    mock_obj = {"Keys": ["test-key"], "Value": mock_message()}
    msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"])
    assert mock_obj["Value"] == msg.value


def test_message_to_all():
    mock_obj = {"Keys": [], "Value": mock_message(), "Tags": []}
    msg = Message(mock_obj["Value"])
    assert isinstance(msg, Message)
    assert mock_obj["Keys"] == msg.keys
    assert mock_obj["Value"] == msg.value
    assert mock_obj["Tags"] == msg.tags


def test_message_to_drop():
    mock_obj = {"Keys": [], "Value": b"", "Tags": [DROP]}
    msg = Message(b"").to_drop()
    assert isinstance(msg, Message)
    assert mock_obj["Keys"] == msg.keys
    assert mock_obj["Value"] == msg.value
    assert mock_obj["Tags"] == msg.tags


def test_message_to():
    mock_obj = {"Keys": ["__KEY__"], "Value": mock_message(), "Tags": ["__TAG__"]}
    msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"], tags=mock_obj["Tags"])
    assert isinstance(msg, Message)
    assert mock_obj["Keys"] == msg.keys
    assert mock_obj["Value"] == msg.value
    assert mock_obj["Tags"] == msg.tags
