"""
Test that MapStreamAsyncServer streams messages incrementally even when the
user handler yields via a regular for-loop (no await between yields).

Regression test for https://github.com/numaproj/numaflow-python/issues/342

Root cause: asyncio.Queue.put() on an unbounded queue never suspends, so the
MapFn consumer task was starved and couldn't stream responses to gRPC until
the handler completed. Fix: asyncio.sleep(0) after each put in the servicer.
"""

import logging
import threading
import time
from collections import deque
from collections.abc import AsyncIterable

import grpc
import pytest

from pynumaflow import setup_logging
from pynumaflow.mapstreamer import Datum, MapStreamAsyncServer, Message
from pynumaflow.proto.mapper import map_pb2_grpc
from tests.conftest import create_async_loop, start_async_server, teardown_async_server
from tests.mapstream.utils import request_generator

LOGGER = setup_logging(__name__)

pytestmark = pytest.mark.integration

SOCK_PATH = "unix:///tmp/async_map_stream_streaming.sock"

NUM_MESSAGES = 5
PRODUCE_INTERVAL_SECS = 0.2


async def slow_streaming_handler(keys: list[str], datum: Datum) -> AsyncIterable[Message]:
    """
    Handler that produces messages from a background thread with a delay
    between each, and yields them via a tight for-loop with NO await.
    This is the pattern from issue #342.
    """
    messages: deque[Message] = deque()

    def _produce():
        for i in range(NUM_MESSAGES):
            messages.append(Message(f"msg-{i}".encode(), keys=keys))
            time.sleep(PRODUCE_INTERVAL_SECS)

    thread = threading.Thread(target=_produce)
    thread.start()

    while thread.is_alive():
        # Tight loop: regular for, no await — the pattern that triggers #342
        while messages:
            yield messages.popleft()

    thread.join()
    while messages:
        yield messages.popleft()


async def _start_server(udfs):
    server = grpc.aio.server()
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    server.add_insecure_port(SOCK_PATH)
    logging.info("Starting server on %s", SOCK_PATH)
    await server.start()
    return server, SOCK_PATH


@pytest.fixture(scope="module")
def streaming_server():
    loop = create_async_loop()
    server_obj = MapStreamAsyncServer(map_stream_instance=slow_streaming_handler)
    udfs = server_obj.servicer
    server = start_async_server(loop, _start_server(udfs))
    yield loop
    teardown_async_server(loop, server)


@pytest.fixture()
def streaming_stub(streaming_server):
    return map_pb2_grpc.MapStub(grpc.insecure_channel(SOCK_PATH))


def test_messages_stream_incrementally(streaming_stub):
    """
    Verify that messages are streamed to the client as they are produced,
    not batched until the handler completes.

    The handler produces NUM_MESSAGES messages with PRODUCE_INTERVAL_SECS between
    each. If streaming works, the first message should arrive well before the
    last one is produced (total production time = NUM_MESSAGES * PRODUCE_INTERVAL_SECS).
    """
    generator_response = streaming_stub.MapFn(
        request_iterator=request_generator(count=1, session=1)
    )

    # Consume handshake
    handshake = next(generator_response)
    assert handshake.handshake.sot

    # Collect messages with their arrival timestamps
    arrival_times = []
    result_count = 0
    for msg in generator_response:
        if hasattr(msg, "status") and msg.status.eot:
            continue
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
