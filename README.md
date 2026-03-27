# numaflow-python

Python SDK for Numaflow.

## `pynumaflow`
Pure Python SDK implementation for Numaflow - [pynumaflow](packages/pynumaflow/README.md)

## `pynumaflow-lite`

Coming shortly (Rust based Python SDK) with better performance

## Example Use Cases
### AsyncIO Reduce Example

Note: This example uses the `asyncio` library to demonstrate how to use the `ExecutorPool` class for parallel processing.

```python
import asyncio
from pynumaflow import ExecutorPool

async def worker(num):
    # Simulate some work
    await asyncio.sleep(1)
    return num * num

async def main():
    # Create an ExecutorPool instance
    executor_pool = ExecutorPool()

    # Submit tasks to the executor pool
    tasks = [executor_pool.submit(worker, i) for i in range(10)]

    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks)

    # Print the results
    print(results)

# Run the main function
asyncio.run(main())
```

Note: The `ExecutorPool` class is used to manage a pool of worker threads or processes that can be used to execute tasks concurrently. In this example, we create an instance of `ExecutorPool`, submit tasks to it using the `submit` method, and then wait for all tasks to complete using the `gather` function.