"""
Test that ReduceStreamAsyncServer streams messages incrementally even when the
user handler writes to the output queue in a tight loop (no meaningful await
between puts).

Regression test for https://github.com/numaproj/numaflow-python/issues/350

Root cause: The SDK's write_to_global_queue reads from the per-task
NonBlockingIterator and writes to the global result queue. Both are backed by
unbounded asyncio.Queues, so neither await truly suspends. This starves the
consumer task that reads from the global queue and streams responses to gRPC,
causing all messages to arrive at once after the handler completes.

Fix: asyncio.sleep(0) after each put in write_to_global_queue.
"""

import logging
import threading
import time
from collections.abc import AsyncIterable

import grpc
import pytest

from pynumaflow import setup_logging
from pynumaflow.reducestreamer import (
    Message,
    Datum,
    ReduceStreamAsyncServer,
    ReduceStreamer,
    Metadata,
)
from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from pynumaflow.shared.asynciter import NonBlockingIterator
from tests.conftest import create_async_loop, start_async_server, teardown_async_server
from tests.testing_utils import (
    mock_message,
    mock_interval_window_start,
    mock_interval_window_end,
    get_time_args,
)

LOGGER = setup_logging(__name__)

pytestmark = pytest.mark.integration

SOCK_PATH = "unix:///tmp/reduce_stream_streaming.sock"

NUM_MESSAGES = 5
PRODUCE_INTERVAL_SECS = 0.2


class SlowStreamingReducer(ReduceStreamer):
    """
    Handler that produces messages from a background thread with a delay
    between each, and writes them to the output queue in a tight loop.
    This mirrors the pattern from issue #342/#350: the user's code has no
    meaningful await between output.put() calls, and the messages are
    produced slowly by a background thread.
    """

    def __init__(self):
        pass

    async def handler(
        self,
        keys: list[str],
        datums: AsyncIterable[Datum],
        output: NonBlockingIterator,
        md: Metadata,
    ):
        # Consume all datums first (required by the protocol)
        async for _ in datums:
            pass

        # Now produce messages from a background thread with delays,
        # and write them to output in a tight loop (no await between puts)
        from collections import deque

        messages: deque[Message] = deque()

        def _produce():
            for i in range(NUM_MESSAGES):
                messages.append(Message(f"msg-{i}".encode(), keys=keys))
                time.sleep(PRODUCE_INTERVAL_SECS)

        thread = threading.Thread(target=_produce)
        thread.start()

        while thread.is_alive():
            # Tight loop: no await between puts — triggers starvation
            while messages:
                await output.put(messages.popleft())

        thread.join()
        while messages:
            await output.put(messages.popleft())


def request_generator(count, request):
    for i in range(count):
        if i % 2:
            request.operation.event = reduce_pb2.ReduceRequest.WindowOperation.Event.OPEN
        else:
            request.operation.event = reduce_pb2.ReduceRequest.WindowOperation.Event.APPEND
        yield request


def start_request():
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
    return reduce_pb2.ReduceRequest(payload=payload, operation=operation)


async def _start_server(udfs):
    server = grpc.aio.server()
    reduce_pb2_grpc.add_ReduceServicer_to_server(udfs, server)
    server.add_insecure_port(SOCK_PATH)
    logging.info("Starting server on %s", SOCK_PATH)
    await server.start()
    return server, SOCK_PATH


@pytest.fixture(scope="module")
def streaming_server():
    loop = create_async_loop()
    server_obj = ReduceStreamAsyncServer(SlowStreamingReducer)
    udfs = server_obj.servicer
    server = start_async_server(loop, _start_server(udfs))
    yield loop
    teardown_async_server(loop, server)


@pytest.fixture()
def streaming_stub(streaming_server):
    return reduce_pb2_grpc.ReduceStub(grpc.insecure_channel(SOCK_PATH))


def test_reduce_stream_messages_stream_incrementally(streaming_stub):
    """
    Verify that messages are streamed to the client as they are produced,
    not batched until the handler completes.

    The handler produces NUM_MESSAGES messages with PRODUCE_INTERVAL_SECS between
    each. If streaming works, the first message should arrive well before the
    last one is produced (total production time = NUM_MESSAGES * PRODUCE_INTERVAL_SECS).
    """
    request = start_request()
    generator_response = streaming_stub.ReduceFn(
        request_iterator=request_generator(count=1, request=request)
    )

    # Collect messages with their arrival timestamps
    arrival_times = []
    result_count = 0
    for msg in generator_response:
        if msg.result.value:
            arrival_times.append(time.monotonic())
            result_count += 1

    assert result_count == NUM_MESSAGES, f"Expected {NUM_MESSAGES} messages, got {result_count}"

    # If messages streamed incrementally, the time span between the first and
    # last arrival should be a significant portion of the total production time.
    # If they were batched, they'd all arrive within a few milliseconds of each other.
    total_production_time = NUM_MESSAGES * PRODUCE_INTERVAL_SECS
    first_to_last = arrival_times[-1] - arrival_times[0]

    # The spread should be at least 40% of production time if streaming works.
    # If batched, the spread would be near zero (~1-5ms).
    min_expected_spread = total_production_time * 0.4
    assert first_to_last >= min_expected_spread, (
        f"Messages arrived too close together ({first_to_last:.3f}s spread), "
        f"expected at least {min_expected_spread:.3f}s. "
        f"This indicates messages were batched instead of streamed. "
        f"Arrival gaps: {[f'{b - a:.3f}s' for a, b in zip(arrival_times, arrival_times[1:])]}"
    )
