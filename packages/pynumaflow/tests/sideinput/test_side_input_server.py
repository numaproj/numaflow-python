import pytest
from google.protobuf import empty_pb2 as _empty_pb2
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.proto.sideinput import sideinput_pb2
from pynumaflow.sideinput import Response, SideInputServer


def retrieve_side_input_handler() -> Response:
    msg = mock_message()
    return Response.broadcast_message(msg)


def retrieve_no_broadcast_handler() -> Response:
    return Response.no_broadcast_message()


def err_retrieve_handler() -> Response:
    raise RuntimeError("Something is fishy!")


def mock_message():
    msg = bytes("test_side_input", encoding="utf-8")
    return msg


@pytest.fixture()
def sideinput_test_server():
    server = SideInputServer(retrieve_side_input_handler)
    services = {sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"]: server.servicer}
    return server_from_dictionary(services, strict_real_time())


def _invoke_retrieve(test_server, metadata_set=None):
    """Helper to invoke RetrieveSideInput unary method."""
    if metadata_set is None:
        metadata_set = {("this_metadata_will_be_skipped", "test_ignore")}
    return test_server.invoke_unary_unary(
        method_descriptor=(
            sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"].methods_by_name[
                "RetrieveSideInput"
            ]
        ),
        invocation_metadata=metadata_set,
        request=_empty_pb2.Empty(),
        timeout=1,
    )


def test_init_with_args():
    my_servicer = SideInputServer(
        side_input_instance=retrieve_side_input_handler,
        sock_path="/tmp/test_side_input.sock",
        max_message_size=1024 * 1024 * 5,
    )
    assert my_servicer.sock_path == "unix:///tmp/test_side_input.sock"
    assert my_servicer.max_message_size == 1024 * 1024 * 5


def test_side_input_err():
    server = SideInputServer(err_retrieve_handler)
    services = {sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"]: server.servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    method = _invoke_retrieve(test_server)
    response, metadata, code, details = method.termination()
    assert code == StatusCode.INTERNAL


def test_is_ready(sideinput_test_server):
    method = sideinput_test_server.invoke_unary_unary(
        method_descriptor=(
            sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"].methods_by_name["IsReady"]
        ),
        invocation_metadata={},
        request=_empty_pb2.Empty(),
        timeout=1,
    )

    response, metadata, code, details = method.termination()
    assert response == sideinput_pb2.ReadyResponse(ready=True)
    assert code == StatusCode.OK


def test_side_input_message(sideinput_test_server):
    """Broadcast message: no_broadcast flag is False and value is mock_message."""
    method = _invoke_retrieve(sideinput_test_server)
    response, metadata, code, details = method.termination()
    assert response.value == mock_message()
    assert code == StatusCode.OK


def test_side_input_no_broadcast():
    """No-broadcast message: no_broadcast flag is True."""
    server = SideInputServer(side_input_instance=retrieve_no_broadcast_handler)
    services = {sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"]: server.servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    method = _invoke_retrieve(test_server)
    response, metadata, code, details = method.termination()
    assert code == StatusCode.OK
    assert response.no_broadcast is True


def test_invalid_input():
    with pytest.raises(TypeError):
        SideInputServer()


def test_max_threads():
    # max cap at 16
    server = SideInputServer(retrieve_side_input_handler, max_threads=32)
    assert server.max_threads == 16

    # use argument provided
    server = SideInputServer(retrieve_side_input_handler, max_threads=5)
    assert server.max_threads == 5

    # defaults to 4
    server = SideInputServer(retrieve_side_input_handler)
    assert server.max_threads == 4
