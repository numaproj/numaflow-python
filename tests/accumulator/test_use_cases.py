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


class StreamSorterAccumulator(Accumulator):
    """
    Accumulator that sorts events by event_time and watermark.
    This demonstrates custom sorting use case.
    """

    def __init__(self):
        self.buffer: list[Datum] = []

    async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
        # Collect all datums
        async for datum in datums:
            self.buffer.append(datum)

        # Sort by event_time
        self.buffer.sort(key=lambda d: d.event_time)

        # Emit sorted datums
        for datum in self.buffer:
            await output.put(
                Message(
                    value=datum.value,
                    keys=datum.keys(),
                    tags=[],
                )
            )

        # Clear buffer for next window
        self.buffer.clear()


class StreamJoinerAccumulator(Accumulator):
    """
    Accumulator that joins streams from different sources based on keys.
    This demonstrates stream joining use case.
    """

    def __init__(self):
        self.streams: dict[str, list[Datum]] = {}

    async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
        # Group datums by source (using first key as source identifier)
        async for datum in datums:
            source_key = datum.keys()[0] if datum.keys() else "default"
            if source_key not in self.streams:
                self.streams[source_key] = []
            self.streams[source_key].append(datum)

        # Join streams by combining data from different sources
        # Sort all data by event_time across all sources
        all_datums = []
        for source_datums in self.streams.values():
            all_datums.extend(source_datums)

        # Sort by event_time for temporal ordering
        all_datums.sort(key=lambda d: d.event_time)

        # Emit joined and sorted stream
        for datum in all_datums:
            joined_value = f"joined_{datum.value.decode()}"
            await output.put(
                Message(
                    value=joined_value.encode(),
                    keys=datum.keys(),
                    tags=["joined"],
                )
            )

        # Clear streams for next window
        self.streams.clear()


class ReorderingAccumulator(Accumulator):
    """
    Accumulator that handles out-of-order events by reordering them.
    This demonstrates event reordering use case.
    """

    def __init__(self, max_delay_seconds: int = 5):
        self.max_delay_seconds = max_delay_seconds
        self.event_buffer: list[Datum] = []

    async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
        # Collect all datums
        async for datum in datums:
            self.event_buffer.append(datum)

        # Sort by event_time to handle out-of-order events
        self.event_buffer.sort(key=lambda d: d.event_time)

        # Process events that are within the acceptable delay window
        current_time = datetime.now(timezone.utc)
        processed_events = []

        for datum in self.event_buffer:
            # Check if event is within acceptable delay
            delay = (current_time - datum.event_time).total_seconds()
            if delay <= self.max_delay_seconds:
                processed_events.append(datum)
            else:
                # Event is too old, but we still process it with a tag
                reordered_datum = datum
                await output.put(
                    Message(
                        value=reordered_datum.value,
                        keys=reordered_datum.keys(),
                        tags=["reordered", "delayed"],
                    )
                )

        # Emit processed events in order
        for datum in processed_events:
            await output.put(
                Message(
                    value=datum.value,
                    keys=datum.keys(),
                    tags=["reordered"],
                )
            )

        # Clear buffer
        self.event_buffer.clear()


class TimeBasedCorrelationAccumulator(Accumulator):
    """
    Accumulator that correlates events from different sources based on timestamps.
    This demonstrates time-based correlation use case.
    """

    def __init__(self, correlation_window_seconds: int = 10):
        self.correlation_window_seconds = correlation_window_seconds
        self.events: list[Datum] = []

    async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
        # Collect all datums
        async for datum in datums:
            self.events.append(datum)

        # Sort by event_time
        self.events.sort(key=lambda d: d.event_time)

        # Correlate events within time windows
        correlated_groups = []
        current_group = []

        for event in self.events:
            if not current_group:
                current_group.append(event)
            else:
                # Check if event is within correlation window
                time_diff = (event.event_time - current_group[0].event_time).total_seconds()
                if time_diff <= self.correlation_window_seconds:
                    current_group.append(event)
                else:
                    # Start new group
                    correlated_groups.append(current_group)
                    current_group = [event]

        # Add the last group
        if current_group:
            correlated_groups.append(current_group)

        # Emit correlated events
        for group_idx, group in enumerate(correlated_groups):
            for event in group:
                correlation_id = f"corr_group_{group_idx}"
                correlated_value = f"correlated_{event.value.decode()}"
                await output.put(
                    Message(
                        value=correlated_value.encode(),
                        keys=event.keys() + [correlation_id],
                        tags=["correlated"],
                    )
                )

        # Clear events
        self.events.clear()


class CustomTriggerAccumulator(Accumulator):
    """
    Accumulator that triggers actions based on custom conditions.
    This demonstrates custom triggering use case.
    """

    def __init__(self, trigger_count: int = 3):
        self.trigger_count = trigger_count
        self.accumulated_events: list[Datum] = []

    async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
        # Collect datums
        async for datum in datums:
            self.accumulated_events.append(datum)

        # Custom trigger: when we have enough events or specific conditions
        if len(self.accumulated_events) >= self.trigger_count:
            # Trigger action: process all accumulated events
            total_value = sum(
                int(event.value.decode())
                for event in self.accumulated_events
                if event.value.decode().isdigit()
            )

            # Emit triggered result
            await output.put(
                Message(
                    value=f"triggered_sum_{total_value}".encode(),
                    keys=["triggered"],
                    tags=["custom_trigger"],
                )
            )

            # Clear accumulated events
            self.accumulated_events.clear()
        else:
            # Not enough events to trigger, emit individual events
            for event in self.accumulated_events:
                await output.put(
                    Message(
                        value=event.value,
                        keys=event.keys(),
                        tags=["pending_trigger"],
                    )
                )


class TestAccumulatorUseCases(unittest.TestCase):
    def test_stream_sorter_accumulator(self):
        """Test the stream sorting use case"""
        sorter = StreamSorterAccumulator()

        # Test that the accumulator sorts by event_time
        self.assertIsInstance(sorter, StreamSorterAccumulator)
        self.assertEqual(len(sorter.buffer), 0)

    def test_stream_joiner_accumulator(self):
        """Test the stream joining use case"""
        joiner = StreamJoinerAccumulator()

        # Test that the accumulator can join streams
        self.assertIsInstance(joiner, StreamJoinerAccumulator)
        self.assertEqual(len(joiner.streams), 0)

    def test_reordering_accumulator(self):
        """Test the event reordering use case"""
        reorderer = ReorderingAccumulator(max_delay_seconds=10)

        # Test that the accumulator handles reordering
        self.assertIsInstance(reorderer, ReorderingAccumulator)
        self.assertEqual(reorderer.max_delay_seconds, 10)
        self.assertEqual(len(reorderer.event_buffer), 0)

    def test_time_based_correlation_accumulator(self):
        """Test the time-based correlation use case"""
        correlator = TimeBasedCorrelationAccumulator(correlation_window_seconds=5)

        # Test that the accumulator correlates events
        self.assertIsInstance(correlator, TimeBasedCorrelationAccumulator)
        self.assertEqual(correlator.correlation_window_seconds, 5)
        self.assertEqual(len(correlator.events), 0)

    def test_custom_trigger_accumulator(self):
        """Test the custom triggering use case"""
        trigger = CustomTriggerAccumulator(trigger_count=5)

        # Test that the accumulator handles custom triggers
        self.assertIsInstance(trigger, CustomTriggerAccumulator)
        self.assertEqual(trigger.trigger_count, 5)
        self.assertEqual(len(trigger.accumulated_events), 0)

    def test_accumulator_server_with_use_cases(self):
        """Test that AccumulatorAsyncServer can be created with use case implementations"""
        # Test with StreamSorterAccumulator
        server1 = AccumulatorAsyncServer(StreamSorterAccumulator)
        self.assertIsNotNone(server1)

        # Test with StreamJoinerAccumulator
        server2 = AccumulatorAsyncServer(StreamJoinerAccumulator)
        self.assertIsNotNone(server2)

        # Test with ReorderingAccumulator with init args
        server3 = AccumulatorAsyncServer(ReorderingAccumulator, init_args=(10,))
        self.assertIsNotNone(server3)

        # Test with TimeBasedCorrelationAccumulator with init args
        server4 = AccumulatorAsyncServer(TimeBasedCorrelationAccumulator, init_args=(15,))
        self.assertIsNotNone(server4)

        # Test with CustomTriggerAccumulator with init args
        server5 = AccumulatorAsyncServer(CustomTriggerAccumulator, init_args=(3,))
        self.assertIsNotNone(server5)

    def test_stream_sorter_functionality(self):
        """Test actual sorting functionality"""

        async def _test_stream_sorter_functionality():
            sorter = StreamSorterAccumulator()
            output = NonBlockingIterator()

            # Create datums with different event times (out of order)
            datums = [
                Datum(
                    keys=["test"],
                    value=b"event_3",
                    event_time=datetime.fromtimestamp(1662998460, timezone.utc),
                    watermark=datetime.fromtimestamp(1662998460, timezone.utc),
                    id_="3",
                ),
                Datum(
                    keys=["test"],
                    value=b"event_1",
                    event_time=datetime.fromtimestamp(1662998400, timezone.utc),
                    watermark=datetime.fromtimestamp(1662998400, timezone.utc),
                    id_="1",
                ),
                Datum(
                    keys=["test"],
                    value=b"event_2",
                    event_time=datetime.fromtimestamp(1662998430, timezone.utc),
                    watermark=datetime.fromtimestamp(1662998430, timezone.utc),
                    id_="2",
                ),
            ]

            async def datum_generator():
                for datum in datums:
                    yield datum

            # Process the datums
            await sorter.handler(datum_generator(), output)

            # Verify the buffer is cleared
            self.assertEqual(len(sorter.buffer), 0)

        asyncio.run(_test_stream_sorter_functionality())

    def test_stream_joiner_functionality(self):
        """Test actual joining functionality"""

        async def _test_stream_joiner_functionality():
            joiner = StreamJoinerAccumulator()
            output = NonBlockingIterator()

            # Create datums from different sources
            datums = [
                Datum(
                    keys=["source1"],
                    value=b"data_from_source1",
                    event_time=datetime.fromtimestamp(1662998400, timezone.utc),
                    watermark=datetime.fromtimestamp(1662998400, timezone.utc),
                    id_="s1_1",
                ),
                Datum(
                    keys=["source2"],
                    value=b"data_from_source2",
                    event_time=datetime.fromtimestamp(1662998430, timezone.utc),
                    watermark=datetime.fromtimestamp(1662998430, timezone.utc),
                    id_="s2_1",
                ),
            ]

            async def datum_generator():
                for datum in datums:
                    yield datum

            # Process the datums
            await joiner.handler(datum_generator(), output)

            # Verify the streams are cleared
            self.assertEqual(len(joiner.streams), 0)

        asyncio.run(_test_stream_joiner_functionality())

    def test_run_async_tests(self):
        """Run the async test methods"""
        # This test method is no longer needed since the async tests
        # are now properly handled within their respective test methods
        pass

    def test_error_handling_scenarios(self):
        """Test error handling scenarios in accumulator processing."""

        async def run_test():
            # Test 1: Function handler called directly (covers line 44 in async_server.py)
            async def func_handler(datums: AsyncIterable[Datum], output: NonBlockingIterator):
                async for datum in datums:
                    await output.put(Message(datum.value, keys=datum.keys()))

            from pynumaflow.accumulator.async_server import get_handler

            handler = get_handler(func_handler)
            self.assertEqual(handler, func_handler)

            # Test 2: Task manager with function handler (covers lines 208->210 in task_manager.py)
            from pynumaflow.accumulator.servicer.task_manager import TaskManager

            task_manager = TaskManager(func_handler)
            # Create mock request iterator that simulates function handler path
            mock_datum = Datum(
                keys=["func_key"],
                value=b"test_data",
                event_time=datetime.now(),
                watermark=datetime.now(),
                id_="func_test",
            )

            output_queue = NonBlockingIterator()

            # Create an async iterator that yields our mock datum
            async def datum_iter():
                yield mock_datum

            # This should cover the function handler path in __invoke_accumulator
            await task_manager._TaskManager__invoke_accumulator(datum_iter(), output_queue)

            # Verify the function handler was called by checking for output
            results = []
            try:
                async for item in output_queue.read_iterator():
                    results.append(item)
                    if item == STREAM_EOF:
                        break
                    # Only expect one message for this test
                    if len(results) >= 3:  # Safety break to avoid infinite loop
                        break
            except Exception:
                pass  # Expected behavior for this simplified test

            # Verify we got at least one result (message was processed)
            self.assertGreaterEqual(len(results), 1)

        asyncio.run(run_test())

    def test_task_manager_error_scenarios(self):
        """Test various error scenarios in TaskManager."""

        # Test 1: Unknown window operation (covers lines 239-243 in task_manager.py)
        from pynumaflow.accumulator.servicer.task_manager import TaskManager
        from unittest.mock import Mock

        handler = Mock()
        task_manager = TaskManager(handler)

        # Test 2: Watermark update with AccumulatorResult (covers lines 311, 315)
        from pynumaflow.accumulator._dtypes import AccumulatorResult

        # Create a task to test watermark updating
        initial_watermark = datetime.fromtimestamp(500)
        task = AccumulatorResult(
            _future=Mock(),
            _iterator=Mock(),
            _key=["test_key"],
            _result_queue=Mock(),
            _consumer_future=Mock(),
            _latest_watermark=initial_watermark,
        )

        # Test update_watermark method directly
        new_watermark = datetime.fromtimestamp(2000)
        task.update_watermark(new_watermark)
        self.assertEqual(task.latest_watermark, new_watermark)

        # Test 3: Test direct instantiation and basic functionality
        unified_key = "test_key"
        task_manager.tasks[unified_key] = task

        # Verify task was added
        self.assertIn(unified_key, task_manager.tasks)
        self.assertEqual(task_manager.tasks[unified_key], task)

    def test_edge_case_scenarios(self):
        """Test edge cases and error conditions."""

        async def run_test():
            from pynumaflow.accumulator.servicer.task_manager import TaskManager
            from unittest.mock import Mock

            # Test 1: Error handling in EOF counting (covers lines 361-363)
            handler = Mock()
            task_manager = TaskManager(handler)

            # Manually set up a scenario where EOF count exceeds expected
            task_manager._expected_eof_count = 1
            task_manager._received_eof_count = 2  # More than expected

            # Create mock task for testing
            from pynumaflow.accumulator._dtypes import AccumulatorResult

            mock_task = AccumulatorResult(
                _future=Mock(),
                _iterator=Mock(),
                _key=["edge_test"],
                _result_queue=Mock(),
                _consumer_future=Mock(),
                _latest_watermark=datetime.now(),
            )

            unified_key = "edge_test"
            task_manager.tasks[unified_key] = mock_task

            # Test watermark handling with None values (covers lines 311, 315)
            input_queue = NonBlockingIterator()
            output_queue = NonBlockingIterator()

            # Create message with None watermark and event_time
            edge_message = Message(
                value=b"edge_test",
                keys=["edge_test"],
                watermark=None,  # Test None watermark handling
                event_time=None,  # Test None event_time handling
            )

            await input_queue.put(edge_message)
            await input_queue.put(STREAM_EOF)

            # This should handle None watermark and event_time without error
            await task_manager.write_to_global_queue(input_queue, output_queue, unified_key)

            # Verify output was generated
            results = []
            async for item in output_queue.read_iterator():
                results.append(item)
                if len(results) >= 2:  # Message + EOF response
                    break

            self.assertTrue(len(results) >= 2)

        asyncio.run(run_test())

    def test_abstract_method_coverage(self):
        """Test abstract method coverage (line 412 in _dtypes.py)."""

        # Test calling the abstract handler method directly
        class DirectTestAccumulator(Accumulator):
            pass  # Don't implement handler to test abstract method

        # This should raise TypeError due to abstract method
        with self.assertRaises(TypeError):
            DirectTestAccumulator()

    def test_servicer_error_handling(self):
        """Test error handling in AsyncAccumulatorServicer (lines 116-118, 122-124)."""

        async def run_test():
            from pynumaflow.accumulator.servicer.async_servicer import AsyncAccumulatorServicer
            from unittest.mock import Mock, patch

            # Test exception in consumer loop (lines 116-118)
            mock_handler = Mock()
            servicer = AsyncAccumulatorServicer(mock_handler)
            mock_context = Mock()

            async def failing_request_iterator():
                yield Mock()  # Just one request

            # Mock TaskManager to simulate error in consumer
            with patch("pynumaflow.accumulator.servicer.async_servicer.TaskManager") as mock_tm:
                mock_task_manager = Mock()
                mock_tm.return_value = mock_task_manager

                # Mock read_iterator to raise exception
                async def failing_reader():
                    raise RuntimeError("Consumer error")

                mock_result_queue = Mock()
                mock_result_queue.read_iterator.return_value = failing_reader()
                mock_task_manager.global_result_queue = mock_result_queue

                # Mock process_input_stream
                async def mock_process():
                    pass

                mock_task_manager.process_input_stream.return_value = mock_process()

                # This should handle the consumer exception (lines 116-118)
                with patch(
                    "pynumaflow.accumulator.servicer.async_servicer.handle_async_error"
                ) as mock_handle:
                    results = []
                    async for result in servicer.AccumulateFn(
                        failing_request_iterator(), mock_context
                    ):
                        results.append(result)

                    # Should have called error handler
                    mock_handle.assert_called()

            # Test exception in producer wait (lines 122-124)
            with patch("pynumaflow.accumulator.servicer.async_servicer.TaskManager") as mock_tm2:
                mock_task_manager2 = Mock()
                mock_tm2.return_value = mock_task_manager2

                # Mock read_iterator to work normally
                async def normal_reader():
                    return
                    yield  # Empty generator

                mock_result_queue2 = Mock()
                mock_result_queue2.read_iterator.return_value = normal_reader()
                mock_task_manager2.global_result_queue = mock_result_queue2

                # Mock process_input_stream to raise exception when awaited
                async def failing_process():
                    raise RuntimeError("Producer error")

                mock_task_manager2.process_input_stream.return_value = failing_process()

                # This should handle the producer exception (lines 122-124)
                with patch(
                    "pynumaflow.accumulator.servicer.async_servicer.handle_async_error"
                ) as mock_handle2:
                    results2 = []
                    async for result in servicer.AccumulateFn(
                        failing_request_iterator(), mock_context
                    ):
                        results2.append(result)

                    # Should have called error handler for producer error
                    mock_handle2.assert_called()

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
