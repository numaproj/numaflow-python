"""
Shutdown-event tests for the SideInput servicer.

Verifies that the servicer sets shutdown_event and captures the error when the
UDF handler raises, enabling graceful server stop via the watcher thread in
_run_server() instead of a hard process kill.
"""

from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.sideinput.servicer.servicer import SideInputServicer
from pynumaflow.proto.sideinput import sideinput_pb2


def _ok_handler():
    from pynumaflow.sideinput import Response

    return Response.broadcast_message(b"test")


def _err_handler():
    raise RuntimeError("Something is fishy!")


def test_shutdown_event_set_on_handler_error():
    """When the UDF handler raises, the servicer must signal the shutdown event."""
    servicer = SideInputServicer(handler=_err_handler)

    services = {sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"]: servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    method = test_server.invoke_unary_unary(
        method_descriptor=(
            sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"].methods_by_name[
                "RetrieveSideInput"
            ]
        ),
        invocation_metadata={},
        request=_empty_pb2.Empty(),
        timeout=1,
    )

    _, _, code, _ = method.termination()
    assert code == StatusCode.INTERNAL
    assert servicer.shutdown_event.is_set()
    assert servicer.error is not None


def test_shutdown_event_not_set_on_success():
    """On a successful call, shutdown_event must remain unset."""
    servicer = SideInputServicer(handler=_ok_handler)

    services = {sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"]: servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    method = test_server.invoke_unary_unary(
        method_descriptor=(
            sideinput_pb2.DESCRIPTOR.services_by_name["SideInput"].methods_by_name[
                "RetrieveSideInput"
            ]
        ),
        invocation_metadata={},
        request=_empty_pb2.Empty(),
        timeout=1,
    )

    _, _, code, _ = method.termination()
    assert code == StatusCode.OK
    assert not servicer.shutdown_event.is_set()
    assert servicer.error is None
