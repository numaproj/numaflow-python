


#  Async-IO Examples

The following are examples of writing UDFs using asynchronous programming techniques. 

We could define asynchronous programming as the way of executing programming code in a thread without having to wait for an I/O-bound or CPU-bound task to finish. I/O-bound operations could be file-system accesses, HTTP requests, API calls, or database queries. And CPU-bound operations would be actions like encrypting data, complex calculations, image or document management.

We focus on writing UDFs in Python for using numaflow-python sdk and Asyncio library.

### Scenario 1 - Using Async functions
In the file [async_exec.py](async_exec.py), we have covered an example of a UDF for making HTTP requests in an async manner. We have used the aiohttp library to make async calls instead of the standard requests library. 

The following async function returns a coroutine for executing each http request.
```
async def http_request(session, url):
```
We collect all the coroutines that are to be executed in a list, and then run them concurrently using the gather function. The results are then collected, and can be further propagated.
```
results = await asyncio.gather(*tasks)
```


#### To compare the performance of the Async implementation vs the sequential implementation we run and compare the results on the following scenarios on an http server.



|              Input RPU:10 &nbsp; I/O delay: 0              |             Input RPU:10 &nbsp; I/O delay: 200µs             | Input RPU:10 &nbsp; I/O delay: 1sec *                       |
|:----------------------------------------------------------:|:------------------------------------------------------------:|-------------------------------------------------------------|
| <img src="testing_data/async/async_10_0.png" width="500"/> | <img src="testing_data/async/async_10_200.png" width="500"/> | <img src="testing_data/async/async_10_1s.png" width="500"/> |



|             Input RPU:100 &nbsp; I/O delay: 0 *             |            Input RPU:100 &nbsp; I/O delay: 200µs *            | Input RPU:100 &nbsp; I/O delay: 1sec *                       |
|:-----------------------------------------------------------:|:-------------------------------------------------------------:|--------------------------------------------------------------|
| <img src="testing_data/async/async_100_0.png" width="500"/> | <img src="testing_data/async/async_100_200.png" width="500"/> | <img src="testing_data/async/async_100_1s.png" width="500"/> |

* Note: Flat line at time=100 for sync indicates no results for that window in the pipeline

Here we can see that a low throughput, sequential and async have comparable performance, but as the I/O bound nature increases async starts to outperform the sequential implementation considerably. It can be observed that at higher loads, the sequential executions seems to error out and not process any results.

### Scenario 2 - Using legacy functions with executors
Another way of programming for the async paradigm can be the use of ThreadPool or ProcessPool executors (Refer: [executorpool.py](executorpool.py)).  As their names suggest, the **ThreadPool** uses threads internally and they are suitable for I/O bound tasks. Whereas the **ProcessPool** uses processes to execute calls asynchronously and are favorable for tasks with heavy CPU requirement.

One upside of using the executor approach is that we do not need to rewrite the legacy function using async mechanism, rather they can be directly passed to executor. 

Both of the executors create a pool of _max_workers_ number of threads/processes. This argument can be specified manually during initialization in accordance with the need of the programmer, otherwise this would use the default value.






We have written a wrapper class for the executor pools implementation with the intent to provide a convenience function, Default executor pool would work just as well in the use case.

```
threadPool = ExecutorPool(exec_type=e_type, max_workers=max_workers)
```

#### To compare the performance of the ThreadPool implementation vs the sequential implementation we run and compare the results on the following scenarios on an http server. Here we use the same http request helper function for both the implementations.

Testing has Max_Workers = 100


|                 Input RPU:10 &nbsp; I/O delay: 0                 |                Input RPU:10 &nbsp; I/O delay: 200µs                | Input RPU:10 &nbsp; I/O delay: 1sec *                            |
|:----------------------------------------------------------------:|:------------------------------------------------------------------:|------------------------------------------------------------------|
| <img src="testing_data/threadpool/thread_10_0.png" width="500"/> | <img src="testing_data/threadpool/thread_10_200.png" width="500"/> | <img src="testing_data/threadpool/thread_10_1.png" width="500"/> |



|                Input RPU:100 &nbsp; I/O delay: 0 *                |               Input RPU:100 &nbsp; I/O delay: 200µs *               | Input RPU:100 &nbsp; I/O delay: 1sec *                             |
|:-----------------------------------------------------------------:|:-------------------------------------------------------------------:|--------------------------------------------------------------------|
| <img src="testing_data/threadpool/thread_100_0.png" width="500"/> | <img src="testing_data/threadpool/thread_100_200.png" width="500"/> | <img src="testing_data/threadpool/thread_100_1s.png" width="500"/> |

* Note: Flat line at time=100 for sync indicates no results for that window in the pipeline

We can see that the ThreadPool implementation gives a considerably better performance at higher throughput and I/O delay. But it should be kept in mind that
the performance will vary according to the max_workers configured for the executor and that should be done carefully according the use case.