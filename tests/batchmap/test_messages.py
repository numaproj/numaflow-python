import unittest

from pynumaflow.batchmapper import Message, DROP, BatchResponse, BatchResponses
from tests.batchmap.test_datatypes import TEST_ID
from tests.testing_utils import mock_message


class TestBatchResponses(unittest.TestCase):
    @staticmethod
    def mock_message_object():
        value = mock_message()
        return Message(value=value)

    def test_init(self):
        batch_responses = BatchResponses()
        batch_response1 = BatchResponse.from_id(TEST_ID)
        batch_response2 = BatchResponse.from_id(TEST_ID + "2")
        batch_responses.append(batch_response1)
        batch_responses.append(batch_response2)
        self.assertEqual(2, len(batch_responses))
        # test indexing
        self.assertEqual(batch_responses[0].id, TEST_ID)
        self.assertEqual(batch_responses[1].id, TEST_ID + "2")
        # test slicing
        resp = batch_responses[0:1]
        self.assertEqual(resp[0].id, TEST_ID)


class TestBatchResponse(unittest.TestCase):
    @staticmethod
    def mock_message_object():
        value = mock_message()
        return Message(value=value)

    def test_init(self):
        batch_response = BatchResponse.from_id(TEST_ID)
        self.assertEqual(batch_response.id, TEST_ID)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            BatchResponse()

    def test_append(self):
        batch_response = BatchResponse.from_id(TEST_ID)
        self.assertEqual(0, len(batch_response.items()))
        batch_response.append(self.mock_message_object())
        self.assertEqual(1, len(batch_response.items()))
        batch_response.append(self.mock_message_object())
        self.assertEqual(2, len(batch_response.items()))

    def test_items(self):
        mock_obj = [
            mock_message(),
            mock_message(),
        ]
        msgs = BatchResponse.with_msgs(TEST_ID, mock_obj)
        self.assertEqual(len(mock_obj), len(msgs.items()))
        self.assertEqual(mock_obj[0], msgs.items()[0])


class TestMessage(unittest.TestCase):
    def test_key(self):
        mock_obj = {"Keys": ["test-key"], "Value": mock_message()}
        msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"])
        print(msg)
        self.assertEqual(mock_obj["Keys"], msg.keys)

    def test_value(self):
        mock_obj = {"Keys": ["test-key"], "Value": mock_message()}
        msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"])
        self.assertEqual(mock_obj["Value"], msg.value)

    def test_message_to_all(self):
        mock_obj = {"Keys": [], "Value": mock_message(), "Tags": []}
        msg = Message(mock_obj["Value"])
        self.assertEqual(Message, type(msg))
        self.assertEqual(mock_obj["Keys"], msg.keys)
        self.assertEqual(mock_obj["Value"], msg.value)
        self.assertEqual(mock_obj["Tags"], msg.tags)

    def test_message_to_drop(self):
        mock_obj = {"Keys": [], "Value": b"", "Tags": [DROP]}
        msg = Message(b"").to_drop()
        self.assertEqual(Message, type(msg))
        self.assertEqual(mock_obj["Keys"], msg.keys)
        self.assertEqual(mock_obj["Value"], msg.value)
        self.assertEqual(mock_obj["Tags"], msg.tags)

    def test_message_to(self):
        mock_obj = {"Keys": ["__KEY__"], "Value": mock_message(), "Tags": ["__TAG__"]}
        msg = Message(value=mock_obj["Value"], keys=mock_obj["Keys"], tags=mock_obj["Tags"])
        self.assertEqual(Message, type(msg))
        self.assertEqual(mock_obj["Keys"], msg.keys)
        self.assertEqual(mock_obj["Value"], msg.value)
        self.assertEqual(mock_obj["Tags"], msg.tags)


if __name__ == "__main__":
    unittest.main()
