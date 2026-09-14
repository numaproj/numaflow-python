import asyncio
import atexit
import gc
import hashlib
import logging
import os
from collections.abc import AsyncIterable
from concurrent.futures import ProcessPoolExecutor
from typing import Tuple, Optional

from pynumaflow.batchmapper import (
    BatchMapper,
    BatchResponse,
    BatchResponses,
    Datum,
)
from pynumaflow.batchmapper import BatchMapAsyncServer
from pynumaflow.mapper import Message

logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)

# Process-level executor
_executor: Optional[ProcessPoolExecutor] = None

gc.enable()


def get_global_executor(proc_count: int = 2) -> ProcessPoolExecutor:
    """Get or create global ProcessPoolExecutor singleton."""
    global _executor
    if _executor is None:
        proc_count = int(os.getenv("NUM_CPU_MULTIPROC", proc_count))
        _executor = ProcessPoolExecutor(max_workers=proc_count, max_tasks_per_child=100)
        _LOGGER.info(
            f"Created global ProcessPoolExecutor (max_workers={proc_count}, "
            f"max_tasks_per_child=100, PID={os.getpid()})"
        )
        atexit.register(_shutdown_executor)
    return _executor


def _shutdown_executor():
    """Shutdown global executor on process exit."""
    global _executor
    if _executor is not None:
        _LOGGER.info("Shutting down global ProcessPoolExecutor")
        _executor.shutdown(wait=True)
        gc.collect()
        _executor = None


def _process_single_message_task(
    task_data: Tuple[str, list, bytes],
) -> Tuple[str, list, bytes, bool]:
    """Worker function executed in ProcessPoolExecutor child process.

    Returns primitive types (msg_id, keys, payload_bytes, should_drop) to safely
    cross process boundaries.
    """
    msg_id, keys, datum_bytes = task_data
    pid = os.getpid()

    try:
        # Decode message
        message_str = datum_bytes.decode("utf-8") if isinstance(datum_bytes, bytes) else datum_bytes
        _LOGGER.info(f"[Worker PID: {pid}] Processing message: {message_str}")

        # CPU-intensive operation: compute hash multiple times
        processed = message_str
        for _ in range(2**10):
            processed = hashlib.sha256(processed.encode()).hexdigest()

        # Simulate transformation
        result = f"PROCESSED[{message_str}]-HASH[{processed[:16]}]"
        result_bytes = result.encode("utf-8")

        _LOGGER.info(f"[Worker PID: {pid}] Processed msg_id={msg_id}: {result}")
        return msg_id, keys, result_bytes, False

    except Exception as e:
        _LOGGER.error(f"[Worker PID: {pid}] Error processing msg_id={msg_id}: {e}")
        return msg_id, keys, b"", True


class ConcurrentSink(BatchMapper):
    """BatchMapper that processes messages concurrently using ProcessPoolExecutor."""

    def __init__(self, max_workers: int = None):
        """Initialize sink with process pool.

        Args:
            max_workers: Number of worker processes. Defaults to CPU count.
        """
        self.executor = get_global_executor(proc_count=max_workers)
        _LOGGER.info(f"Initialized ConcurrentSink with ProcessPoolExecutor (PID={os.getpid()})")

    async def handler(self, datums: AsyncIterable[Datum]) -> BatchResponses:
        """Process batch of datums concurrently in worker processes.

        Args:
            datums: AsyncIterable of Datum objects from the stream.

        Returns:
            BatchResponses with results from all processed messages.
        """
        responses = BatchResponses()
        loop = asyncio.get_running_loop()

        # Collect all datums from the async iterable
        datums_list = []
        async for datum in datums:
            datums_list.append(datum)

        _LOGGER.info(f"Processing batch of {len(datums_list)} messages")

        if not datums_list:
            return responses

        # Submit all tasks to process pool concurrently
        async def submit_task(datum: Datum):
            """Submit a single message to worker pool."""
            task_data = (datum.id, datum.keys, datum.value)
            fut = loop.run_in_executor(self.executor, _process_single_message_task, task_data)
            return await asyncio.wait_for(fut, timeout=30.0)

        # Wait for all results
        results = await asyncio.gather(
            *[submit_task(d) for d in datums_list],
            return_exceptions=True,
        )

        # Construct responses from results
        for datum, res in zip(datums_list, results):
            batch_resp = BatchResponse.from_id(datum.id)

            if isinstance(res, Exception):
                _LOGGER.error(f"Task failed for datum ID: {datum.id}, error: {res}")
                batch_resp.append(Message.to_drop())
            else:
                msg_id, keys, payload_bytes, should_drop = res
                if should_drop:
                    batch_resp.append(Message.to_drop())
                else:
                    batch_resp.append(Message(value=payload_bytes, keys=keys))

            responses.append(batch_resp)

        _LOGGER.info(f"Completed processing, generated {len(responses)} responses")
        return responses


if __name__ == "__main__":
    """
    Example of starting a concurrent sink with process pool.

    To configure the number of worker processes, set NUM_WORKERS env var:
        NUM_WORKERS=4 python example.py
    NB: NUM_WORKERS is a user created ENV variable, not a platform ENV varible
    """
    max_workers = int(os.getenv("NUM_WORKERS", str(os.cpu_count() or 2)))
    sink = ConcurrentSink(max_workers=max_workers)
    grpc_server = BatchMapAsyncServer(batch_mapper_instance=sink, max_threads=1)
    grpc_server.start()
