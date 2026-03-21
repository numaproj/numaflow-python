from pynumaflow.sideinput import Response, SideInput


class ExampleSideInput(SideInput):
    def retrieve_handler(self) -> Response:
        return Response.broadcast_message(b"testMessage")


def test_broadcast_message():
    """
    Test the broadcast_message method,
    where we expect the no_broadcast flag to be False.
    """
    succ_response = Response.broadcast_message(b"2")
    assert not succ_response.no_broadcast
    assert b"2" == succ_response.value


def test_no_broadcast_message():
    """
    Test the no_broadcast_message method,
    where we expect the no_broadcast flag to be True.
    """
    succ_response = Response.no_broadcast_message()
    assert succ_response.no_broadcast


def test_side_input_class_call():
    """Test that the __call__ functionality for the class works,
    ie the class instance can be called directly to invoke the handler function
    """
    side_input_instance = ExampleSideInput()
    # make a call to the class directly
    ret = side_input_instance()
    assert b"testMessage" == ret.value
    # make a call to the handler
    ret_handler = side_input_instance.retrieve_handler()
    # Both responses should be equal
    assert ret == ret_handler
