import unittest

from pynumaflow.sideinput import Response


class TestResponse(unittest.TestCase):
    """
    Test the Response class for SideInput
    """

    def test_broadcast_message(self):
        """
        Test the broadcast_message method,
        where we expect the no_broadcast flag to be False.
        """
        succ_response = Response.broadcast_message(b"2")
        self.assertFalse(succ_response.no_broadcast)
        self.assertEqual(b"2", succ_response.value)

    def test_no_broadcast_message(self):
        """
        Test the no_broadcast_message method,
        where we expect the no_broadcast flag to be True.
        """
        succ_response = Response.no_broadcast_message()
        self.assertTrue(succ_response.no_broadcast)


if __name__ == "__main__":
    unittest.main()
