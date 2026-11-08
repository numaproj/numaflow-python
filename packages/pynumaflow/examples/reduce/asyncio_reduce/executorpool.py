import asyncio
import concurrent.futures
import os
import requests
import time
from collections.abc import AsyncIterable

from pynumaflow import setup_logging
from pynumaflow.reducer import (
    Messages,
    Message,
    Datum,
    Metadata,
    ReduceAsyncServer,
    Reducer,
)

_LOGGER = setup_logging(__name__)


class ReduceHandler(Reducer):
    def __init__(self, exec_pool):
        self.exec_pool = exec_pool

    async def handler(self, key: list[str], datums: AsyncIterable[Datum], md: Metadata) -> Messages:
        """
        handler function for executing operations on the messages received by the reduce vertex
        Here the function is used to execute certain blocking operations to demonstrate the use of
        asyncio with ThreadPool and ProcessPool Executor
        """
        interval_window = md.interval_window
        tasks = []
        start_time = time.time()
        async for _ in datums:
            url = "http://host.docker.internal:9888/ping"
            fut = self.exec_pool.submit(blocking_call, url)
            tasks.append(fut)
        await self.exec_pool.gather(tasks)
        end_time = time.time()
        msg = (
            f"batch_time:{end_time - start_time} interval_window_start:{interval_window.start} "
            f"interval_window_end:{interval_window.end}"
        )
        return Messages(Message(keys=key, value=str.encode(msg)))


class ExecutorPool:
    """
    A class used to create an Executor Pool using the "concurrent.futures" library
    Currently ThreadPool and ProcessPool are supported

    ...

    Attributes
    ----------
    exec_type : "thread" or "process"
        thread - Used to create a ThreadPool Executor
        process - Used to create a ProcessPool Executor

    max_workers : Number of workers in the executor pool
        the max number of threads/process to be created in the pool

    Methods
    -------
    async submit(func, *args)
        Submit the given function to the executor, returns a futures object

    async gather(tasks, return_exceptions, return_when)
        Wait for the coroutines to execute and gather the results

    close(wait)
         Shutdown the executor
    """

    def __init__(self, exec_type="thread", max_workers=None):
        if exec_type == "thread":
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        elif exec_type == "process":
            self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)

    def submit(self, func, *args):
        return asyncio.get_running_loop().run_in_executor(self.executor, func, args)

    async def gather(self, tasks, return_exceptions=False, return_when="ALL_COMPLETED"):
        completed, pending = await asyncio.wait(tasks, return_when=return_when)
        results = [t.result() for t in completed]
        if return_exceptions:
            results += [t.exception() for t in completed if t.exception()]
        # self.tasks = []
        return results

    def close(self, wait=True):
        self.executor.shutdown(wait)


"""
Invoke a I/O blocking call using http requests
"""


def blocking_call(url):
    url = url[0]
    res = requests.get(url)
    try:
        res = res.json()
        return res["message"]
    except Exception as e:
        _LOGGER.error("HTTP request error: %s", e)
        return "Error"


"""
EXEC_TYPE is environment var to decide the type of executor to be used by the UDF
"""
if __name__ == "__main__":
    e_type = os.getenv("EXEC_TYPE", "thread").lower()
    mx_workers = os.getenv("MAX_WORKERS")
    if mx_workers:
        mx_workers = int(mx_workers)

    exec_pool = ExecutorPool(exec_type=e_type, max_workers=mx_workers)
    grpc_server = ReduceAsyncServer(ReduceHandler, init_args=(exec_pool,))
    grpc_server.start()
