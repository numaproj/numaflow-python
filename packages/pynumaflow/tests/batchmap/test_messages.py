import pytest

from pynumaflow.batchmapper import Message, DROP, BatchResponse, BatchResponses
from tests.batchmap.test_datatypes import TEST_ID
from tests.testing_utils import mock_message


def _mock_message_object():
    value = mock_message()
    return Message(value=value)


def test_batch_responses_init():
    batch_responses = BatchResponses()
    batch_response1 = BatchResponse.from_id(TEST_ID)
    batch_response2 = BatchResponse.from_id(TEST_ID + "2")
    batch_responses.append(batch_response1)
    batch_responses.append(batch_response2)
    assert 2 == len(batch_responses)
    # test indexing
    assert batch_responses[0].id == TEST_ID
    assert batch_responses[1].id == TEST_ID + "2"
    # test slicing
    resp = batch_responses[0:1]
    assert resp[0].id == TEST_ID


def test_batch_response_init():
    batch_response = BatchResponse.from_id(TEST_ID)
    assert batch_response.id == TEST_ID


def test_batch_response_invalid_input():
    with pytest.raises(TypeError):
        BatchResponse()


def test_batch_response_append():
    batch_response = BatchResponse.from_id(TEST_ID)
    assert 0 == len(batch_response.items())
    batch_response.append(_mock_message_object())
    assert 1 == len(batch_response.items())
    batch_response.append(_mock_message_object())
    assert 2 == len(batch_response.items())


def test_batch_response_items():
    mock_obj = [
        mock_message(),
        mock_message(),
    ]
    msgs = BatchResponse.with_msgs(TEST_ID, mock_obj)
    assert len(mock_obj) == len(msgs.items())
    assert mock_obj[0] == msgs.items()[0]


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
