"""
Shutdown-event tests for the async Source servicer.

Each test verifies that when a CancelledError is raised during an RPC
(simulating a SIGTERM-triggered task cancellation), the servicer:
  - sets the shutdown_event
  - does NOT store an error (CancelledError is not a UDF fault)
"""

import asyncio
from unittest import mock

from pynumaflow.sourcer.servicer.async_servicer import AsyncSourceServicer
from pynumaflow.sourcer._dtypes import (
    Sourcer,
    ReadRequest,
    AckRequest,
    NackRequest,
    PendingResponse,
    PartitionsResponse,
)
from pynumaflow.shared.asynciter import NonBlockingIterator
from pynumaflow.proto.sourcer import source_pb2

# ---------------------------------------------------------------------------
# Minimal handler that never raises on its own — individual tests will
# override specific methods or inject CancelledError via the request stream.
# ---------------------------------------------------------------------------


class _StubSource(Sourcer):
    async def read_handler(self, datum: ReadRequest, output: NonBlockingIterator):
        pass

    async def ack_handler(self, ack_request: AckRequest):
        pass

    async def nack_handler(self, nack_request: NackRequest):
        pass

    async def pending_handler(self) -> PendingResponse:
        return PendingResponse(count=0)

    async def active_partitions_handler(self) -> PartitionsResponse:
        return PartitionsResponse(partitions=[])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _collect(async_gen):
    """Drain an async generator and return the collected items."""
    results = []
    async for item in async_gen:
        results.append(item)
    return results


# ---------------------------------------------------------------------------
# ReadFn — streaming RPC.  CancelledError raised from the request iterator
# after the handshake has been sent.
# ---------------------------------------------------------------------------


def test_shutdown_on_read_cancelled_error():
    """CancelledError during ReadFn request iteration should set
    shutdown_event but NOT store an error."""

    async def _run():
        servicer = AsyncSourceServicer(source_handler=_StubSource())
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        # Build a request iterator that sends the handshake then raises
        # CancelledError on the next iteration (simulating SIGTERM).
        async def _cancelled_request_iter():
            yield source_pb2.ReadRequest(handshake=source_pb2.Handshake(sot=True))
            raise asyncio.CancelledError()

        ctx = mock.MagicMock()
        await _collect(servicer.ReadFn(_cancelled_request_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AckFn — streaming RPC.  Same pattern as ReadFn.
# ---------------------------------------------------------------------------


def test_shutdown_on_ack_cancelled_error():
    """CancelledError during AckFn request iteration should set
    shutdown_event but NOT store an error."""

    async def _run():
        servicer = AsyncSourceServicer(source_handler=_StubSource())
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        async def _cancelled_request_iter():
            yield source_pb2.AckRequest(handshake=source_pb2.Handshake(sot=True))
            raise asyncio.CancelledError()

        ctx = mock.MagicMock()
        await _collect(servicer.AckFn(_cancelled_request_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# NackFn — unary RPC.  We make the handler raise CancelledError.
# ---------------------------------------------------------------------------


def test_shutdown_on_nack_cancelled_error():
    """CancelledError during NackFn handler should set
    shutdown_event but NOT store an error."""

    async def _run():
        handler = _StubSource()

        async def _cancelled_nack(nack_request):
            raise asyncio.CancelledError()

        handler.nack_handler = _cancelled_nack

        servicer = AsyncSourceServicer(source_handler=handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        # Build a valid NackRequest proto with at least one offset.
        offset = source_pb2.Offset(offset=b"test", partition_id=0)
        request = source_pb2.NackRequest(request=source_pb2.NackRequest.Request(offsets=[offset]))
        ctx = mock.MagicMock()
        # NackFn is a coroutine (not an async generator), so we await it.
        await servicer.NackFn(request, ctx)

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PendingFn — unary RPC.  Handler raises CancelledError.
# ---------------------------------------------------------------------------


def test_shutdown_on_pending_cancelled_error():
    """CancelledError during PendingFn handler should set
    shutdown_event but NOT store an error."""

    async def _run():
        handler = _StubSource()

        async def _cancelled_pending():
            raise asyncio.CancelledError()

        handler.pending_handler = _cancelled_pending

        servicer = AsyncSourceServicer(source_handler=handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        ctx = mock.MagicMock()
        await servicer.PendingFn(mock.MagicMock(), ctx)

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PartitionsFn — unary RPC.  Handler raises CancelledError.
# ---------------------------------------------------------------------------


def test_shutdown_on_partitions_cancelled_error():
    """CancelledError during PartitionsFn handler should set
    shutdown_event but NOT store an error."""

    async def _run():
        handler = _StubSource()

        async def _cancelled_partitions():
            raise asyncio.CancelledError()

        handler.active_partitions_handler = _cancelled_partitions

        servicer = AsyncSourceServicer(source_handler=handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        ctx = mock.MagicMock()
        await servicer.PartitionsFn(mock.MagicMock(), ctx)

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())
