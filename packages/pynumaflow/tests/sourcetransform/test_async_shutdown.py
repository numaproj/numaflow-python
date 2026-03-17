"""
Shutdown-event tests for the async SourceTransform servicer.

Covers the CancelledError and BaseException handlers in SourceTransformFn.
"""

import asyncio
from unittest import mock

from pynumaflow.sourcetransformer.servicer._async_servicer import SourceTransformAsyncServicer
from pynumaflow.sourcetransformer import Datum, Messages, Message
from tests.testing_utils import mock_new_event_time


async def async_transform_handler(keys: list[str], datum: Datum) -> Messages:
    return Messages(Message(datum.value, mock_new_event_time(), keys=keys))


async def _collect(async_gen):
    results = []
    async for item in async_gen:
        results.append(item)
    return results


def test_shutdown_on_cancelled_error():
    """CancelledError during SourceTransformFn should set shutdown_event, no error stored."""

    async def _run():
        servicer = SourceTransformAsyncServicer(handler=async_transform_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        async def _cancelled_iter():
            raise asyncio.CancelledError()
            yield

        ctx = mock.MagicMock()
        await _collect(servicer.SourceTransformFn(_cancelled_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())


def test_shutdown_on_handler_error():
    """BaseException in SourceTransformFn should set shutdown_event and store error."""

    async def _run():
        servicer = SourceTransformAsyncServicer(handler=async_transform_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        async def _error_iter():
            raise RuntimeError("unexpected error")
            yield

        ctx = mock.MagicMock()
        await _collect(servicer.SourceTransformFn(_error_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is not None
        assert "unexpected error" in repr(servicer._error)

    asyncio.run(_run())
