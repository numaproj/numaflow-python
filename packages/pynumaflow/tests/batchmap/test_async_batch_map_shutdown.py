"""
Shutdown-event tests for the async BatchMap servicer.

Tests verify that the servicer correctly handles:
1. CancelledError during MapFn iteration (SIGTERM scenario)
2. Handler RuntimeError caught by the outer BaseException handler
"""

import asyncio
from unittest import mock

from pynumaflow.batchmapper.servicer.async_servicer import AsyncBatchMapServicer
from pynumaflow.batchmapper import BatchResponses
from pynumaflow.proto.mapper import map_pb2
from tests.batchmap.utils import request_generator


async def _noop_handler(datums) -> BatchResponses:
    async for _ in datums:
        pass
    return BatchResponses()


async def _err_handler(datums) -> BatchResponses:
    raise RuntimeError("handler blew up")


async def _collect(async_gen):
    """Drain an async generator into a list."""
    results = []
    async for item in async_gen:
        results.append(item)
    return results


def test_shutdown_on_cancelled_error():
    """CancelledError during MapFn should set shutdown_event but NOT store an error."""

    async def _run():
        servicer = AsyncBatchMapServicer(handler=_noop_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        async def _cancelled_iter():
            raise asyncio.CancelledError()
            yield  # make it an async generator

        ctx = mock.MagicMock()
        responses = await _collect(servicer.MapFn(_cancelled_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())


def test_shutdown_on_handler_error():
    """Handler RuntimeError caught by the outer BaseException handler; shutdown_event
    is set and the error is stored on the servicer."""

    async def _run():
        servicer = AsyncBatchMapServicer(handler=_err_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        # Build an async request iterator with handshake + data + EOT
        sync_datums = list(request_generator(count=2, session=1, handshake=True))

        async def _request_iter():
            for d in sync_datums:
                yield d

        ctx = mock.MagicMock()
        responses = await _collect(servicer.MapFn(_request_iter(), ctx))

        # First response should be the handshake ack
        assert responses[0].handshake.sot

        assert shutdown_event.is_set()
        assert servicer._error is not None
        assert "handler blew up" in repr(servicer._error)

    asyncio.run(_run())
