"""
Shutdown-event tests for the synchronous Map servicer.

Mirrors the sinker shutdown test pattern (tests/sink/test_server.py lines 345-461).
Each test verifies that the servicer sets shutdown_event (and optionally captures the
error) under a specific failure mode, enabling graceful server stop via the watcher
thread in _run_server() instead of a hard process kill.
"""

from unittest import mock

import grpc
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.mapper._servicer._sync_servicer import SyncMapServicer
from pynumaflow.proto.mapper import map_pb2
from tests.map.utils import map_handler, err_map_handler, get_test_datums


def test_shutdown_event_set_on_handler_error():
    """When the UDF handler raises, the servicer must signal the shutdown event."""
    servicer = SyncMapServicer(handler=err_map_handler)

    services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    test_datums = get_test_datums(handshake=True)

    method = test_server.invoke_stream_stream(
        method_descriptor=(map_pb2.DESCRIPTOR.services_by_name["Map"].methods_by_name["MapFn"]),
        invocation_metadata={},
        timeout=2,
    )

    for d in test_datums:
        method.send_request(d)
    method.requests_closed()

    while True:
        try:
            method.take_response()
        except ValueError:
            break

    _, code, _ = method.termination()
    assert code == StatusCode.INTERNAL
    assert servicer.shutdown_event.is_set()
    assert servicer.error is not None


def test_shutdown_event_set_on_handshake_error():
    """Missing handshake must also signal the shutdown event."""
    servicer = SyncMapServicer(handler=map_handler)

    services = {map_pb2.DESCRIPTOR.services_by_name["Map"]: servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    # Send a data message without a handshake first
    test_datums = get_test_datums(handshake=False)

    method = test_server.invoke_stream_stream(
        method_descriptor=(map_pb2.DESCRIPTOR.services_by_name["Map"].methods_by_name["MapFn"]),
        invocation_metadata={},
        timeout=1,
    )

    for d in test_datums:
        method.send_request(d)
    method.requests_closed()

    while True:
        try:
            method.take_response()
        except ValueError:
            break

    _, code, details = method.termination()
    assert code == StatusCode.INTERNAL
    assert "MapFn: expected handshake as the first message" in details
    assert servicer.shutdown_event.is_set()
    assert servicer.error is not None


def test_shutdown_event_set_on_stream_close_before_handshake():
    """grpc.RpcError on the first read (before handshake): shutdown_event set,
    result_queue is None so close is skipped."""
    servicer = SyncMapServicer(handler=map_handler)

    def _cancelled_iter():
        raise grpc.RpcError()
        yield  # make it a generator

    responses = list(servicer.MapFn(_cancelled_iter(), mock.MagicMock()))

    assert responses == []
    assert servicer.shutdown_event.is_set()
    # Not a UDF error — error stays None
    assert servicer.error is None


def test_shutdown_event_set_on_stream_close_mid_processing():
    """grpc.RpcError mid-processing: result_queue is closed (unblocking the handler
    thread) and shutdown_event is set."""
    servicer = SyncMapServicer(handler=map_handler)

    test_datums = get_test_datums(handshake=True)

    def _cancelled_iter():
        yield test_datums[0]  # handshake
        yield test_datums[1]  # first data message
        raise grpc.RpcError()

    responses = list(servicer.MapFn(_cancelled_iter(), mock.MagicMock()))

    # Should have at least the handshake response
    assert responses[0].handshake.sot
    assert servicer.shutdown_event.is_set()
    # Not a UDF error — error stays None
    assert servicer.error is None
