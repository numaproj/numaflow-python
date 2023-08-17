from pynumaflow.sideinput import SideInputResponse


def retrieve_side_input_handler() -> SideInputResponse:
    msg = mock_message()
    return SideInputResponse(msg)


def err_retrieve_handler() -> SideInputResponse:
    raise RuntimeError("Something is fishy!")


def mock_message():
    msg = bytes("test_side_input", encoding="utf-8")
    return msg
