import unittest

from pynumaflow.sideinput import Response, SideInputClass


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


class ExampleSideInput(SideInputClass):
    def retrieve_handler(self) -> Response:
        return Response.broadcast_message(b"testMessage")


class TestSideInputClass(unittest.TestCase):
    def setUp(self) -> None:
        # Create a side input class instance
        self.side_input_instance = ExampleSideInput()

    def test_side_input_class_call(self):
        """Test that the __call__ functionality for the class works,
        ie the class instance can be called directly to invoke the handler function
        """
        # make a call to the class directly
        ret = self.side_input_instance()
        self.assertEqual(b"testMessage", ret.value)
        # make a call to the handler
        ret_handler = self.side_input_instance.retrieve_handler()
        # Both responses should be equal
        self.assertEqual(ret, ret_handler)


if __name__ == "__main__":
    unittest.main()
