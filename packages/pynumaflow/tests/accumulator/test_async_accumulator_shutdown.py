"""
Shutdown-event tests for the async Accumulator servicer.

Tests verify that the servicer correctly handles:
1. CancelledError during consumer iteration (SIGTERM scenario)
2. BaseException during consumer iteration (unexpected error)
3. CancelledError during producer await (SIGTERM scenario)
4. BaseException during producer await (producer task error)
5. Exception object yielded from the result queue (task_manager error)
"""

import asyncio
from unittest import mock

from pynumaflow.accumulator.servicer.async_servicer import AsyncAccumulatorServicer
from pynumaflow.shared.asynciter import NonBlockingIterator


async def noop_handler(datums, output):
    async for _ in datums:
        pass


async def _empty_request_iter():
    return
    yield  # make it an async generator


async def _collect(async_gen):
    """Collect all items from an async generator."""
    results = []
    async for item in async_gen:
        results.append(item)
    return results


def test_shutdown_event_on_consumer_cancelled_error():
    """CancelledError while reading the result queue (e.g. SIGTERM) should
    set shutdown_event but NOT store an error."""

    async def _run():
        servicer = AsyncAccumulatorServicer(handler=noop_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        mock_task_manager = mock.MagicMock()

        async def _cancelled_reader():
            raise asyncio.CancelledError()
            yield

        mock_task_manager.global_result_queue.read_iterator.return_value = _cancelled_reader()
        mock_task_manager.process_input_stream = mock.AsyncMock()

        with mock.patch(
            "pynumaflow.accumulator.servicer.async_servicer.TaskManager",
            return_value=mock_task_manager,
        ):
            ctx = mock.MagicMock()
            await _collect(servicer.AccumulateFn(_empty_request_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())


def test_shutdown_event_on_consumer_base_exception():
    """BaseException on the result queue should set shutdown_event AND store the error."""

    async def _run():
        servicer = AsyncAccumulatorServicer(handler=noop_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        mock_task_manager = mock.MagicMock()

        async def _error_reader():
            raise RuntimeError("unexpected consumer error")
            yield

        mock_task_manager.global_result_queue.read_iterator.return_value = _error_reader()
        mock_task_manager.process_input_stream = mock.AsyncMock()

        with mock.patch(
            "pynumaflow.accumulator.servicer.async_servicer.TaskManager",
            return_value=mock_task_manager,
        ):
            ctx = mock.MagicMock()
            await _collect(servicer.AccumulateFn(_empty_request_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is not None
        assert "unexpected consumer error" in repr(servicer._error)

    asyncio.run(_run())


def test_shutdown_event_on_producer_cancelled_error():
    """CancelledError when awaiting the producer task should set shutdown_event
    but NOT store an error."""

    async def _run():
        servicer = AsyncAccumulatorServicer(handler=noop_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        mock_task_manager = mock.MagicMock()

        async def _empty_reader():
            return
            yield

        mock_task_manager.global_result_queue.read_iterator.return_value = _empty_reader()

        async def _cancelled_producer(_):
            raise asyncio.CancelledError()

        mock_task_manager.process_input_stream = _cancelled_producer

        with mock.patch(
            "pynumaflow.accumulator.servicer.async_servicer.TaskManager",
            return_value=mock_task_manager,
        ):
            ctx = mock.MagicMock()
            await _collect(servicer.AccumulateFn(_empty_request_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is None

    asyncio.run(_run())


def test_shutdown_event_on_producer_base_exception():
    """BaseException from the producer task should set shutdown_event AND store the error."""

    async def _run():
        servicer = AsyncAccumulatorServicer(handler=noop_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        mock_task_manager = mock.MagicMock()

        async def _empty_reader():
            return
            yield

        mock_task_manager.global_result_queue.read_iterator.return_value = _empty_reader()

        async def _error_producer(_):
            raise RuntimeError("producer blew up")

        mock_task_manager.process_input_stream = _error_producer

        with mock.patch(
            "pynumaflow.accumulator.servicer.async_servicer.TaskManager",
            return_value=mock_task_manager,
        ):
            ctx = mock.MagicMock()
            await _collect(servicer.AccumulateFn(_empty_request_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is not None
        assert "producer blew up" in repr(servicer._error)

    asyncio.run(_run())


def test_shutdown_event_on_result_queue_exception_message():
    """When the result queue yields a BaseException (from task_manager),
    shutdown_event should be set and the error stored."""

    async def _run():
        servicer = AsyncAccumulatorServicer(handler=noop_handler)
        shutdown_event = asyncio.Event()
        servicer.set_shutdown_event(shutdown_event)

        mock_task_manager = mock.MagicMock()

        async def _exception_in_queue():
            yield RuntimeError("handler error from task manager")

        mock_task_manager.global_result_queue.read_iterator.return_value = (
            _exception_in_queue()
        )
        mock_task_manager.process_input_stream = mock.AsyncMock()

        with mock.patch(
            "pynumaflow.accumulator.servicer.async_servicer.TaskManager",
            return_value=mock_task_manager,
        ):
            ctx = mock.MagicMock()
            await _collect(servicer.AccumulateFn(_empty_request_iter(), ctx))

        assert shutdown_event.is_set()
        assert servicer._error is not None
        assert "handler error from task manager" in repr(servicer._error)

    asyncio.run(_run())
