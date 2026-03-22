"""
Shutdown-event tests for the multiproc SourceTransform servicer.

These tests verify that the SourceTransformServicer (as used by
SourceTransformMultiProcServer) correctly sets shutdown_event on error,
enabling coordinated graceful shutdown across all worker processes via
the shared multiprocessing.Event.
"""

from unittest import mock

import grpc
from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time

from pynumaflow.sourcetransformer.multiproc_server import SourceTransformMultiProcServer
from pynumaflow.proto.sourcetransformer import transform_pb2
from tests.conftest import drain_responses, send_test_requests
from tests.sourcetransform.utils import transform_handler, err_transform_handler, get_test_datums


def test_shutdown_event_set_on_handler_error():
    """When the UDF handler raises, the servicer must signal the shutdown event."""
    server = SourceTransformMultiProcServer(source_transform_instance=err_transform_handler)
    servicer = server.servicer

    services = {transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"]: servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    test_datums = get_test_datums(handshake=True)

    method = test_server.invoke_stream_stream(
        method_descriptor=(
            transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"].methods_by_name[
                "SourceTransformFn"
            ]
        ),
        invocation_metadata={},
        timeout=2,
    )

    send_test_requests(method, test_datums)
    drain_responses(method)

    _, code, _ = method.termination()
    assert code == StatusCode.INTERNAL
    assert servicer.shutdown_event.is_set()
    assert servicer.error is not None


def test_shutdown_event_set_on_handshake_error():
    """Missing handshake must also signal the shutdown event."""
    server = SourceTransformMultiProcServer(source_transform_instance=transform_handler)
    servicer = server.servicer

    services = {transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"]: servicer}
    test_server = server_from_dictionary(services, strict_real_time())

    test_datums = get_test_datums(handshake=False)

    method = test_server.invoke_stream_stream(
        method_descriptor=(
            transform_pb2.DESCRIPTOR.services_by_name["SourceTransform"].methods_by_name[
                "SourceTransformFn"
            ]
        ),
        invocation_metadata={},
        timeout=1,
    )

    send_test_requests(method, test_datums)
    drain_responses(method)

    _, code, details = method.termination()
    assert code == StatusCode.INTERNAL
    assert "SourceTransformFn: expected handshake message" in details
    assert servicer.shutdown_event.is_set()
    assert servicer.error is not None


def test_shutdown_event_set_on_stream_close_before_handshake():
    """grpc.RpcError on the first read (before handshake): shutdown_event set,
    result_queue is None so close is skipped."""
    server = SourceTransformMultiProcServer(source_transform_instance=transform_handler)
    servicer = server.servicer

    def _cancelled_iter():
        raise grpc.RpcError()
        yield  # make it a generator

    responses = list(servicer.SourceTransformFn(_cancelled_iter(), mock.MagicMock()))

    assert responses == []
    assert servicer.shutdown_event.is_set()
    assert servicer.error is None


def test_shutdown_event_set_on_stream_close_mid_processing():
    """grpc.RpcError mid-processing: result_queue is closed (unblocking the handler
    thread) and shutdown_event is set."""
    server = SourceTransformMultiProcServer(source_transform_instance=transform_handler)
    servicer = server.servicer

    test_datums = get_test_datums(handshake=True)

    def _cancelled_iter():
        yield test_datums[0]  # handshake
        yield test_datums[1]  # first data message
        raise grpc.RpcError()

    responses = list(servicer.SourceTransformFn(_cancelled_iter(), mock.MagicMock()))

    assert responses[0].handshake.sot
    assert servicer.shutdown_event.is_set()
    assert servicer.error is None
