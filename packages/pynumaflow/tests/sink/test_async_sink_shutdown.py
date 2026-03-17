"""
Shutdown-event tests for the async Sink servicer.

Tests verify that the servicer correctly handles:
1. CancelledError during SinkFn iteration (SIGTERM scenario)
2. Handler RuntimeError caught by the outer BaseException handler
"""

import asyncio
from collections.abc import AsyncIterable
from unittest import mock

from pynumaflow.sinker.servicer.async_servicer import AsyncSinkServicer
from pynumaflow.sinker import Datum, Responses, Response
from pynumaflow.proto.sinker import sink_pb2
from tests.sink.test_async_sink import request_generator


async def _noop_handler(datums: AsyncIterable[Datum]) -> Responses:
    responses = Responses()
    async for msg in datums:
        responses.append(Response.as_success(msg.id))
    return responses


async def _err_handler(datums: AsyncIterable[Datum]) -> Responses:
    raise RuntimeError("handler blew up")


async def _collect(async_gen):
    """Drain an async generator into a list."""
    results = []
    async for item in async_gen:
        results.append(item)
    return results


def test_shutdown_on_cancelled_error():
    """CancelledError during SinkFn should set shutdown_event but NOT store an error."""

    async def _run():
        servicer = AsyncSinkServicer(handler=_noop_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        async def _cancelled_iter():
            raise asyncio.CancelledError()
            yield  # make it an async generator

        ctx = mock.MagicMock()
        responses = await _collect(servicer.SinkFn(_cancelled_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())


def test_shutdown_on_handler_error():
    """Handler RuntimeError caught by the outer BaseException handler; shutdown_event
    is set and the error is stored on the servicer."""

    async def _run():
        servicer = AsyncSinkServicer(handler=_err_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        # Build an async request iterator with handshake + data + EOT
        sync_datums = list(request_generator(count=2, req_type="success", session=1, handshake=True))

        async def _request_iter():
            for d in sync_datums:
                yield d

        ctx = mock.MagicMock()
        responses = await _collect(servicer.SinkFn(_request_iter(), ctx))

        # First response should be the handshake ack
        assert responses[0].handshake.sot

        assert shutdown_event.is_set()
        assert servicer._error is not None
        assert "handler blew up" in repr(servicer._error)

    asyncio.run(_run())
