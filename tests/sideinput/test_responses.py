import unittest

from pynumaflow.proto.serving import store_pb2
from pynumaflow.servingstore import Payload
from pynumaflow.sideinput import Response


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
        print(succ_response.value)

    def test_no_broadcast_message(self):
        """
        Test the no_broadcast_message method,
        where we expect the no_broadcast flag to be True.
        """
        succ_response = Response.no_broadcast_message()
        self.assertTrue(succ_response.no_broadcast)


#
# class ExampleSideInput(SideInput):
#     def retrieve_handler(self) -> Response:
#         return Response.broadcast_message(b"testMessage")
#
#
# class TestSideInputClass(unittest.TestCase):
#     def setUp(self) -> None:
#         # Create a side input class instance
#         self.side_input_instance = ExampleSideInput()
#
#     def test_side_input_class_call(self):
#         """Test that the __call__ functionality for the class works,
#         ie the class instance can be called directly to invoke the handler function
#         """
#         # make a call to the class directly
#         ret = self.side_input_instance()
#         self.assertEqual(b"testMessage", ret.value)
#         # make a call to the handler
#         ret_handler = self.side_input_instance.retrieve_handler()
#         # Both responses should be equal
#         self.assertEqual(ret, ret_handler)


if __name__ == "__main__":
    unittest.main()
