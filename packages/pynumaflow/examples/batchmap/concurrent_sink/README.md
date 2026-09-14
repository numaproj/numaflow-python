# Concurrent Sink Example

Demonstrates concurrent processing of batch messages using `ProcessPoolExecutor` with `BatchMapAsyncServer`.
Use batch processing only when it makes sense. In some scenarios, batch processing may **not** be the most efficient approach, and processing data items one by one could be a better option. 
The burden of concurrent processing of the data will rely on the UDF implementation in this use case.

## Overview

This example shows a workload by offloading CPU-intensive work to a pool of worker processes. Each message in a batch is processed independently and in parallel, improving throughput for compute-heavy operations.

## Key Features

- **Parallel Processing**: Uses `ProcessPoolExecutor` to process multiple messages concurrently
- **Async/Await**: Leverages `asyncio` for non-blocking task submission and collection
- **Cross-Process Safety**: Returns only primitive types from worker processes (strings, bytes, booleans)
- **Graceful Degradation**: Drops messages on processing errors
- **Configurable Workers**: Adjust worker count via `NUM_WORKERS` environment variable

## Architecture

1. **Main Process (AsyncIO Loop)**
   - Receives batch of datums from Numaflow
   - Submits each message to `ProcessPoolExecutor` via `loop.run_in_executor()`
   - Collects results concurrently with `asyncio.gather()`
   - Constructs and returns `BatchResponses`

2. **Worker Processes**
   - Execute `_process_message_task()` in isolation
   - Perform CPU-intensive work (hash computation in this example)
   - Return primitive types only (no Pynumaflow objects)

## Usage

```bash
# Start with default worker count (CPU count)
python example.py

# Start with custom worker count
NUM_WORKERS=4 python example.py
```

## Message Flow

```
Input Batch → Async Drain → Submit to Workers → Collect Results → BatchResponses
                ↓                   ↓                    ↓                 ↓
        [D1, D2, D3, ...]  Worker Pool (P1-Pn)  Task Futures  [BR1, BR2, BR3, ...]
```

## Processing Example

**Input Message**: `"hello world"`

**Processing**:
1. Decode UTF-8
2. Compute SHA256 hash 100 times iteratively
3. Return processed result with truncated hash

**Output**: `"PROCESSED[hello world]-HASH[<truncated_hash>]"`

## Error Handling

- **Process Timeout**: Drops message (returns empty payload)
- **Exception in Worker**: Logs error, drops message
- **Invalid Input**: Gracefully handled with try-except

## Configuration

### Environment Variables

- `NUM_WORKERS`: Number of worker processes (default: CPU count)

### Resource Tuning

For CPU-bound work:
- Set `NUM_WORKERS` ≈ CPU cores
- Larger batches improve throughput
- Monitor worker process memory usage

## Comparison to Alternatives

| Approach | Pros | Cons |
|----------|------|------|
| **ProcessPool** (this) | True parallelism, CPU-bound work | Process overhead, IPC serialization |
| **ThreadPool** | Lower overhead | GIL contention, not for CPU work |
| **AsyncIO** | Lightweight, I/O-bound | Single-threaded, no true parallelism |

## Related Examples

- `examples/map/multiproc_map/`: Single-message multiprocessing mapper
- `examples/batchmap/flatmap/`: Basic batch mapping without parallelism
- `examples/sink/async_log/`: Async sink without process pool
