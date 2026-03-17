"""
Shutdown-event tests for the async Map servicer.

Tests verify that the servicer correctly handles:
1. CancelledError during MapFn iteration (SIGTERM scenario)
2. Handler RuntimeError surfaced via result queue
3. Bad handshake raising MapError
"""

import asyncio
from unittest import mock

from pynumaflow.mapper._servicer._async_servicer import AsyncMapServicer
from pynumaflow.mapper._dtypes import Messages, Message, Datum
from pynumaflow.proto.mapper import map_pb2
from tests.map.utils import get_test_datums


async def _noop_handler(keys: list[str], datum: Datum) -> Messages:
    return Messages(Message(b"ok", keys=keys))


async def _err_handler(keys: list[str], datum: Datum) -> Messages:
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
        servicer = AsyncMapServicer(handler=_noop_handler)
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
    """Handler RuntimeError surfaces via the result queue; shutdown_event is set
    and the error is stored on the servicer."""

    async def _run():
        servicer = AsyncMapServicer(handler=_err_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        # Build an async request iterator with handshake + one datum
        test_datums = get_test_datums(handshake=True)

        async def _request_iter():
            for d in test_datums:
                yield d

        ctx = mock.MagicMock()
        responses = await _collect(servicer.MapFn(_request_iter(), ctx))

        # First response should be the handshake ack
        assert responses[0].handshake.sot

        assert shutdown_event.is_set()
        assert servicer._error is not None
        assert "handler blew up" in repr(servicer._error)

    asyncio.run(_run())


def test_shutdown_on_handshake_error():
    """Missing handshake raises MapError; shutdown_event is set and the error is stored."""

    async def _run():
        servicer = AsyncMapServicer(handler=_noop_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        # Send data messages without a handshake first
        test_datums = get_test_datums(handshake=False)

        async def _request_iter():
            for d in test_datums:
                yield d

        ctx = mock.MagicMock()
        responses = await _collect(servicer.MapFn(_request_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is not None
        assert "expected handshake" in repr(servicer._error)

    asyncio.run(_run())
