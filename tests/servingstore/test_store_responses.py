import unittest

from pynumaflow.proto.serving import store_pb2
from pynumaflow.servingstore import Payload


class TestPayload(unittest.TestCase):
    """
    Test the Response class for SideInput
    """

    def test_create_payload(self):
        """
        Test the new payload method,
        """
        x = store_pb2.Payload(origin="abc1", value=bytes("test_put", encoding="utf-8"))
        succ_response = Payload(origin=x.origin, value=x.value)
        self.assertEqual(succ_response.value, x.value)


if __name__ == "__main__":
    unittest.main()
