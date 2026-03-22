import asyncio
import logging
from collections.abc import AsyncIterable
from unittest.mock import MagicMock

import grpc
import pytest

from pynumaflow import setup_logging
from pynumaflow._constants import WIN_START_TIME, WIN_END_TIME
from pynumaflow.reducestreamer import (
    Message,
    Datum,
    ReduceStreamAsyncServer,
    ReduceStreamer,
    Metadata,
)
from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from pynumaflow.reducestreamer.servicer.async_servicer import AsyncReduceStreamServicer
from pynumaflow.reducestreamer.servicer.task_manager import TaskManager
from pynumaflow.shared.asynciter import NonBlockingIterator
from tests.conftest import create_async_loop, start_async_server, teardown_async_server
from tests.testing_utils import (
    mock_message,
    mock_interval_window_start,
    mock_interval_window_end,
    get_time_args,
)

pytestmark = pytest.mark.integration

LOGGER = setup_logging(__name__)

SOCK_PATH = "unix:///tmp/reduce_stream_err.sock"


def request_generator(count, request, resetkey: bool = False):
    for i in range(count):
        if resetkey:
            request.payload.keys.extend([f"key-{i}"])
        yield request


def start_request(multiple_window: False) -> (Datum, tuple):
    event_time_timestamp, watermark_timestamp = get_time_args()
    window = reduce_pb2.Window(
        start=mock_interval_window_start(),
        end=mock_interval_window_end(),
        slot="slot-0",
    )
    payload = reduce_pb2.ReduceRequest.Payload(
        value=mock_message(),
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
    )
    operation = reduce_pb2.ReduceRequest.WindowOperation(
        event=reduce_pb2.ReduceRequest.WindowOperation.Event.APPEND,
        windows=[window],
    )
    if multiple_window:
        operation = reduce_pb2.ReduceRequest.WindowOperation(
            event=reduce_pb2.ReduceRequest.WindowOperation.Event.APPEND,
            windows=[window, window],
        )

    request = reduce_pb2.ReduceRequest(
        payload=payload,
        operation=operation,
    )
    metadata = (
        (WIN_START_TIME, f"{mock_interval_window_start()}"),
        (WIN_END_TIME, f"{mock_interval_window_end()}"),
    )
    return request, metadata


class ExampleClass(ReduceStreamer):
    def __init__(self, counter):
        self.counter = counter

    async def handler(
        self,
        keys: list[str],
        datums: AsyncIterable[Datum],
        output: NonBlockingIterator,
        md: Metadata,
    ):
        # print(md.start)
        async for _ in datums:
            self.counter += 1
            if self.counter > 2:
                msg = f"counter:{self.counter}"
                await output.put(Message(str.encode(msg), keys=keys))
                self.counter = 0
                raise RuntimeError("Got a runtime error from reduce handler.")
        raise RuntimeError("Got a runtime error from reduce handler.")
        msg = f"counter:{self.counter}"
        await output.put(Message(str.encode(msg), keys=keys))


async def reduce_handler_func(
    keys: list[str],
    datums: AsyncIterable[Datum],
    output: NonBlockingIterator,
    md: Metadata,
):
    counter = 0
    async for _ in datums:
        counter += 1
        if counter > 2:
            msg = f"counter:{counter}"
            await output.put(Message(str.encode(msg), keys=keys))
            counter = 0
    raise RuntimeError("Got a runtime error from reduce handler.")
    msg = f"counter:{counter}"
    await output.put(Message(str.encode(msg), keys=keys))


def NewAsyncReduceStreamer():
    server_instance = ReduceStreamAsyncServer(ExampleClass, init_args=(0,))
    udfs = server_instance.servicer
    return udfs


async def _start_server(udfs):
    server = grpc.aio.server()
    reduce_pb2_grpc.add_ReduceServicer_to_server(udfs, server)
    server.add_insecure_port(SOCK_PATH)
    logging.info("Starting server on %s", SOCK_PATH)
    await server.start()
    return server, SOCK_PATH


@pytest.fixture(scope="module")
def async_reduce_stream_err_server():
    """Module-scoped fixture: starts an async gRPC reduce stream error server."""
    loop = create_async_loop()
    udfs = NewAsyncReduceStreamer()
    server = start_async_server(loop, _start_server(udfs))
    yield loop
    teardown_async_server(loop, server)


@pytest.fixture()
def reduce_stream_err_stub(async_reduce_stream_err_server):
    """Returns a ReduceStub connected to the running async error server."""
    return reduce_pb2_grpc.ReduceStub(grpc.insecure_channel(SOCK_PATH))


def test_reduce(reduce_stream_err_stub) -> None:
    request, metadata = start_request(multiple_window=False)
    generator_response = None
    try:
        generator_response = reduce_stream_err_stub.ReduceFn(
            request_iterator=request_generator(count=10, request=request),
        )
        counter = 0
        for _ in generator_response:
            counter += 1
    except BaseException as err:
        assert "Got a runtime error from reduce handler." in str(err)
        return
    pytest.fail("Expected an exception.")


def test_reduce_window_len(reduce_stream_err_stub) -> None:
    request, metadata = start_request(multiple_window=True)
    generator_response = None
    try:
        generator_response = reduce_stream_err_stub.ReduceFn(
            request_iterator=request_generator(count=10, request=request)
        )
        counter = 0
        for _ in generator_response:
            counter += 1
    except Exception as err:
        assert "reduce append operation error: invalid number of windows" in str(err)
        return
    try:
        request.operation.event = reduce_pb2.ReduceRequest.WindowOperation.Event.OPEN
        generator_response = reduce_stream_err_stub.ReduceFn(
            request_iterator=request_generator(count=10, request=request)
        )
        counter = 0
        for _ in generator_response:
            counter += 1
    except Exception as err:
        assert "reduce create operation error: invalid number of windows" in str(err)
        return
    pytest.fail("Expected an exception.")


# --- Standalone test functions (not part of the TestCase) ---


async def _emit_one_handler(keys, datums, output, md):
    """Handler that emits one message eagerly, then blocks reading remaining datums."""
    await output.put(Message(b"result", keys=keys))
    async for _ in datums:
        pass


def test_cancelled_error_in_consumer_loop():
    """athrow(CancelledError) at the yield point exercises the except CancelledError branch."""
    servicer = AsyncReduceStreamServicer(_emit_one_handler)
    shutdown_event = asyncio.Event()
    servicer.set_shutdown_event(shutdown_event)
    request, _ = start_request(multiple_window=False)

    async def _run():
        async def requests():
            yield request
            await asyncio.sleep(999)

        gen = servicer.ReduceFn(requests(), MagicMock())
        # Drive the pipeline until the handler's message is yielded.
        await gen.__anext__()
        # Simulate task cancellation (e.g. SIGTERM) at the yield point.
        try:
            await gen.athrow(asyncio.CancelledError())
        except StopAsyncIteration:
            pass

    asyncio.run(_run())
    assert shutdown_event.is_set()
    assert servicer._error is None


def test_base_exception_in_consumer_loop():
    """athrow(ValueError) at the yield point exercises the except BaseException branch."""
    servicer = AsyncReduceStreamServicer(_emit_one_handler)
    shutdown_event = asyncio.Event()
    servicer.set_shutdown_event(shutdown_event)
    request, _ = start_request(multiple_window=False)

    async def _run():
        async def requests():
            yield request
            await asyncio.sleep(999)

        ctx = MagicMock()
        gen = servicer.ReduceFn(requests(), ctx)
        await gen.__anext__()
        try:
            await gen.athrow(ValueError("boom"))
        except StopAsyncIteration:
            pass
        return ctx

    ctx = asyncio.run(_run())
    assert shutdown_event.is_set()
    assert isinstance(servicer._error, ValueError)
    ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)


_original_process_input_stream = TaskManager.process_input_stream


def test_cancelled_error_awaiting_producer():
    """CancelledError from the producer task after it finishes its real work."""
    servicer = AsyncReduceStreamServicer(_emit_one_handler)
    shutdown_event = asyncio.Event()
    servicer.set_shutdown_event(shutdown_event)
    request, _ = start_request(multiple_window=False)

    async def raise_after_real_work(self, request_iterator):
        await _original_process_input_stream(self, request_iterator)
        raise asyncio.CancelledError()

    TaskManager.process_input_stream = raise_after_real_work
    try:

        async def _run():
            async def requests():
                yield request

            gen = servicer.ReduceFn(requests(), MagicMock())
            async for _ in gen:
                pass

        asyncio.run(_run())
    finally:
        TaskManager.process_input_stream = _original_process_input_stream

    assert shutdown_event.is_set()
    assert servicer._error is None


def test_base_exception_awaiting_producer():
    """BaseException from the producer task after it finishes its real work."""
    servicer = AsyncReduceStreamServicer(_emit_one_handler)
    shutdown_event = asyncio.Event()
    servicer.set_shutdown_event(shutdown_event)
    request, _ = start_request(multiple_window=False)

    async def raise_after_real_work(self, request_iterator):
        await _original_process_input_stream(self, request_iterator)
        raise RuntimeError("producer boom")

    TaskManager.process_input_stream = raise_after_real_work
    try:

        async def _run():
            async def requests():
                yield request

            ctx = MagicMock()
            gen = servicer.ReduceFn(requests(), ctx)
            async for _ in gen:
                pass
            return ctx

        ctx = asyncio.run(_run())
    finally:
        TaskManager.process_input_stream = _original_process_input_stream

    assert shutdown_event.is_set()
    assert isinstance(servicer._error, RuntimeError)
    ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)


async def _blocking_handler(keys, datums, output, md):
    """Handler that blocks forever reading datums (never finishes on its own)."""
    async for _ in datums:
        pass
    await output.put(Message(b"done", keys=keys))


def _make_reduce_request(operation_event):
    """Create a ReduceRequest DTO (not raw protobuf) matching what datum_generator produces."""
    from pynumaflow.reducestreamer._dtypes import ReduceRequest as ReduceRequestDTO

    event_time_timestamp, watermark_timestamp = get_time_args()
    window = reduce_pb2.Window(
        start=mock_interval_window_start(),
        end=mock_interval_window_end(),
        slot="slot-0",
    )
    payload = Datum(
        keys=["test_key"],
        value=mock_message(),
        event_time=event_time_timestamp.ToDatetime(),
        watermark=watermark_timestamp.ToDatetime(),
    )
    return ReduceRequestDTO(
        operation=operation_event,
        windows=[window],
        payload=payload,
    )


def test_cancel_and_await_remaining_tasks_on_post_processing_error():
    """
    When a BaseException occurs during post-processing (after the input stream
    is exhausted), the TaskManager should cancel and await all remaining task
    futures that are still running.
    """
    from unittest.mock import patch
    from pynumaflow.reducestreamer._dtypes import WindowOperation

    tm = TaskManager(_blocking_handler)
    req = _make_reduce_request(int(WindowOperation.OPEN))

    async def _run():
        async def requests():
            yield req

        # Patch stream_send_eof to raise after the task is created but before
        # it completes, so the task futures are still running when the except
        # block executes.
        with patch.object(tm, "stream_send_eof", side_effect=RuntimeError("send_eof boom")):
            await tm.process_input_stream(requests())

        # Verify tasks were actually created
        assert len(tm.get_tasks()) > 0, "tasks should have been created"

        # After process_input_stream returns, verify the error was placed in
        # the global result queue.
        reader = tm.global_result_queue.read_iterator()
        first_item = await reader.__anext__()
        assert isinstance(first_item, RuntimeError)
        assert "send_eof boom" in str(first_item)

        # Verify all task futures completed (cancelled or finished).
        for task in tm.get_tasks():
            assert task.future.done(), "task.future should be done after cleanup"
            assert task.consumer_future.done(), "task.consumer_future should be done after cleanup"

    asyncio.run(_run())


def test_cancel_and_await_with_already_done_futures():
    """
    When post-processing fails but some futures are already done,
    the cleanup code should skip cancellation for those (fut.done() is True).
    """
    from unittest.mock import patch
    from pynumaflow.reducestreamer._dtypes import WindowOperation
    from pynumaflow._constants import STREAM_EOF

    async def _fast_handler(keys, datums, output, md):
        """Handler that finishes immediately without reading datums."""
        await output.put(Message(b"fast", keys=keys))

    tm = TaskManager(_fast_handler)
    req = _make_reduce_request(int(WindowOperation.OPEN))

    async def _run():
        async def requests():
            yield req

        original_send_eof = tm.stream_send_eof

        async def send_eof_then_wait_and_raise():
            # Let the real stream_send_eof run (sends EOF to handler input)
            await original_send_eof()
            # Wait for all task futures to complete so they are .done()
            for task in tm.get_tasks():
                await task.future
                await task.result_queue.put(STREAM_EOF)
                await task.consumer_future
            raise RuntimeError("late post-processing error")

        with patch.object(tm, "stream_send_eof", side_effect=send_eof_then_wait_and_raise):
            await tm.process_input_stream(requests())

        # Verify tasks were actually created
        assert len(tm.get_tasks()) > 0, "tasks should have been created"

        # Verify cleanup completed without issues
        for task in tm.get_tasks():
            assert task.future.done()
            assert task.consumer_future.done()

    asyncio.run(_run())
