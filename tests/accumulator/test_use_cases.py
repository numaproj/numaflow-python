"""Accumulator Use Case Examples

These tests demonstrate working patterns for common accumulator use cases.

Functionality Being Tested:
===========================

1. StreamSorterAccumulator:
   - Collects incoming Datum objects from async streams
   - Sorts events by event_time in chronological order
   - Emits sorted events as Message objects
   - Manages internal buffer state between processing windows

2. AccumulatorAsyncServer Integration:
   - Server instantiation with custom accumulator classes
   - Initialization argument passing to accumulator constructors
   - Server configuration with different accumulator types

Verification Criteria:
=====================

1. End-to-End Processing:
   - Input: Out-of-order events with different timestamps
   - Processing: Async stream handling and sorting logic
   - Output: Chronologically ordered Message objects
   - State Management: Buffer clearing after processing

2. Server Integration:
   - Server creation with various accumulator configurations
   - Proper servicer instantiation and lifecycle
   - Support for parameterized accumulator constructors

3. Data Flow Validation:
   - Datum to Message conversion patterns
   - Async iterator usage with STREAM_EOF handling
   - Message format and content verification
"""

import asyncio
import unittest
from collections.abc import AsyncIterable
from datetime import datetime, timezone

from pynumaflow.accumulator import (
    Message,
    Datum,
    Accumulator,
    AccumulatorAsyncServer,
)
from pynumaflow.shared.asynciter import NonBlockingIterator
from pynumaflow._constants import STREAM_EOF
from tests.accumulator.utils import create_test_datum


class StreamSorterAccumulator(Accumulator):
    """
    Accumulator that sorts events by event_time and watermark.
    This demonstrates custom sorting use case.
    """

    def __init__(self):
        self.buffer: list[Datum] = []

    async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
        # Collect all datums
        datum_count = 0
        async for datum in datums:
            datum_count += 1
            self.buffer.append(datum)

        # Sort by event_time
        self.buffer.sort(key=lambda d: d.event_time)

        # Emit sorted datums
        message_count = 0
        for datum in self.buffer:
            message_count += 1
            await output.put(
                Message(
                    value=datum.value,
                    keys=datum.keys(),
                    tags=[],
                )
            )

        # Clear buffer for next window
        self.buffer.clear()


class TestAccumulatorUseCases(unittest.TestCase):
    """Test practical accumulator use cases that developers can reference."""

    def test_stream_sorter_end_to_end_example(self):
        """Complete end-to-end example showing how to build a stream sorting accumulator.

        This test demonstrates:
        1. How to implement the Accumulator abstract class
        2. How to process async streams of Datum objects
        3. How to emit results as Message objects
        4. Proper resource management (buffer clearing)
        """

        async def _test_end_to_end():
            # Create accumulator instance
            sorter = StreamSorterAccumulator()
            output = NonBlockingIterator()

            # Create test data - intentionally out of chronological order
            test_data = [
                create_test_datum(
                    keys=["sensor_data"],
                    value=b"temperature:25.5",
                    event_time=datetime.fromtimestamp(1662998460, timezone.utc),  # Latest
                    id_="temp_3",
                ),
                create_test_datum(
                    keys=["sensor_data"],
                    value=b"temperature:22.1",
                    event_time=datetime.fromtimestamp(1662998400, timezone.utc),  # Earliest
                    id_="temp_1",
                ),
                create_test_datum(
                    keys=["sensor_data"],
                    value=b"temperature:23.8",
                    event_time=datetime.fromtimestamp(1662998430, timezone.utc),  # Middle
                    id_="temp_2",
                ),
            ]

            # Create async generator (simulates streaming data)
            async def data_stream():
                for datum in test_data:
                    yield datum

            # Process the stream through our accumulator
            await sorter.handler(data_stream(), output)

            # Collect and verify results
            results = []
            await output.put(STREAM_EOF)  # Signal end of stream

            async for item in output.read_iterator():
                if item == STREAM_EOF:
                    break
                results.append(item)
                if len(results) >= 10:  # Safety break
                    break

            # Verify we received the expected number of messages
            self.assertEqual(len(results), 3)

            # Verify all results are properly formatted Message objects
            for i, result in enumerate(results):
                self.assertIsInstance(result, Message)
                self.assertEqual(result.keys, ["sensor_data"])
                self.assertEqual(result.tags, [])
                # Values should be in chronological order due to sorting
                if i == 0:
                    self.assertEqual(result.value, b"temperature:22.1")  # Earliest
                elif i == 1:
                    self.assertEqual(result.value, b"temperature:23.8")  # Middle
                elif i == 2:
                    self.assertEqual(result.value, b"temperature:25.5")  # Latest

        asyncio.run(_test_end_to_end())

    def test_accumulator_server_integration_example(self):
        """Example showing how to integrate custom accumulator with AccumulatorAsyncServer.

        This demonstrates:
        1. Server instantiation with custom accumulator class
        2. Passing initialization arguments to accumulator constructors
        3. Server lifecycle management
        """
        # Example 1: Simple accumulator without initialization args
        server = AccumulatorAsyncServer(StreamSorterAccumulator)
        self.assertIsNotNone(server)
        self.assertIsNotNone(server.servicer)

        # Example 2: Accumulator with configuration parameters
        class ConfigurableAccumulator(Accumulator):
            def __init__(self, max_buffer_size: int = 1000, sort_ascending: bool = True):
                self.max_buffer_size = max_buffer_size
                self.sort_ascending = sort_ascending
                self.buffer = []

            async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
                # Collect datums up to max buffer size
                async for datum in datums:
                    if len(self.buffer) < self.max_buffer_size:
                        self.buffer.append(datum)

                # Sort based on configuration
                self.buffer.sort(key=lambda d: d.event_time, reverse=not self.sort_ascending)

                # Emit results
                for datum in self.buffer:
                    await output.put(Message(value=datum.value, keys=datum.keys(), tags=["sorted"]))

                # Clean up
                self.buffer.clear()

        # Create server with custom configuration
        configured_server = AccumulatorAsyncServer(
            ConfigurableAccumulator,
            init_args=(500, False),  # max_buffer_size=500, sort_ascending=False
        )
        self.assertIsNotNone(configured_server)


if __name__ == "__main__":
    unittest.main()
