"""
Shutdown-event tests for the async MapStream servicer.

Tests verify that the servicer correctly handles:
1. CancelledError during MapFn iteration (SIGTERM scenario)
2. Handler RuntimeError surfaced via result queue
"""

import asyncio
from collections.abc import AsyncIterable
from unittest import mock

from pynumaflow.mapstreamer.servicer.async_servicer import AsyncMapStreamServicer
from pynumaflow.mapstreamer._dtypes import Message
from pynumaflow.mapstreamer import Datum
from tests.mapstream.utils import request_generator


async def _noop_handler(keys: list[str], datum: Datum) -> AsyncIterable[Message]:
    yield Message(b"ok", keys=keys)


async def _err_handler(keys: list[str], datum: Datum) -> AsyncIterable[Message]:
    raise RuntimeError("handler blew up")
    yield  # make it an async generator


async def _collect(async_gen):
    """Drain an async generator into a list."""
    results = []
    async for item in async_gen:
        results.append(item)
    return results


def test_shutdown_on_cancelled_error():
    """CancelledError during MapFn should set shutdown_event but NOT store an error."""

    async def _run():
        servicer = AsyncMapStreamServicer(handler=_noop_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        async def _cancelled_iter():
            raise asyncio.CancelledError()
            yield  # make it an async generator

        ctx = mock.MagicMock()
        await _collect(servicer.MapFn(_cancelled_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())


def test_shutdown_on_handler_error():
    """Handler RuntimeError surfaces via the result queue; shutdown_event is set
    and the error is stored on the servicer."""

    async def _run():
        servicer = AsyncMapStreamServicer(handler=_err_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        # Build an async request iterator with handshake + data
        sync_datums = list(request_generator(count=2, handshake=True))

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
