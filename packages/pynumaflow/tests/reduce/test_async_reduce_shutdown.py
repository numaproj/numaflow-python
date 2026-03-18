"""
Shutdown-event tests for the async Reduce servicer.

The ReduceFn method has two try/except blocks where we added shutdown handling:
  1. During request iteration (datum_generator loop)
  2. During task result collection (awaiting futures)

Tests verify that:
  - CancelledError sets shutdown_event but does NOT store an error
  - Handler errors set shutdown_event AND store the error
"""

import asyncio
from collections.abc import AsyncIterable
from unittest import mock

from pynumaflow.reducer.servicer.async_servicer import AsyncReduceServicer
from pynumaflow.reducer._dtypes import (
    Datum,
    Messages,
    Message,
    Metadata,
    Reducer,
)

# ---------------------------------------------------------------------------
# Minimal handler — never raises on its own.
# ---------------------------------------------------------------------------


class _StubReducer(Reducer):
    async def handler(
        self, keys: list[str], datums: AsyncIterable[Datum], md: Metadata
    ) -> Messages:
        async for _ in datums:
            pass
        return Messages(Message(b"done", keys=keys))


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
# Test 1: CancelledError during request iteration
#
# We feed the servicer a request iterator that raises CancelledError.
# This exercises the first except block in ReduceFn.
# ---------------------------------------------------------------------------


def test_shutdown_on_cancelled_error():
    """CancelledError during request iteration should set shutdown_event
    but NOT store an error."""

    async def _run():
        servicer = AsyncReduceServicer(handler=_StubReducer)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        # An async iterator that immediately raises CancelledError,
        # simulating SIGTERM arriving while reading from the gRPC stream.
        async def _cancelled_request_iter():
            raise asyncio.CancelledError()
            yield  # make it an async generator

        ctx = mock.MagicMock()
        await _collect(servicer.ReduceFn(_cancelled_request_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 2: Handler raises a real error during request iteration
#
# We feed a request iterator that raises RuntimeError.
# This exercises the BaseException except block in the first try.
# ---------------------------------------------------------------------------


def test_shutdown_on_handler_error():
    """A real exception during request iteration should set shutdown_event
    AND store the error."""

    async def _run():
        servicer = AsyncReduceServicer(handler=_StubReducer)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        async def _error_request_iter():
            raise RuntimeError("reduce iteration blew up")
            yield

        ctx = mock.MagicMock()
        await _collect(servicer.ReduceFn(_error_request_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is not None
        assert "reduce iteration blew up" in repr(servicer._error)

    asyncio.run(_run())
