# Multiprocessing Map

`pynumaflow` supports only asyncio based Reduce UDFs because we found that the procedural Python is not able to handle 
any substantial traffic.

This features enables the `pynumaflow` developer to utilise multiprocessing capabilities while 
writing UDFs using map function. These are particularly useful for CPU intensive operations,
as it allows for better resource utilisation.

In this mode we would spawn N number (N = Cpu count) of grpc servers in different processes, where each of them
listening on multiple TCP sockets.

To enable multiprocessing mode 

1) Start the multiproc server in the UDF using the following command
2) Provide the optional argument `server_count` to specify the number of
servers to be forked. Defaults to `os.cpu_count` if not provided
```python
if __name__ == "__main__":
    grpc_server = MapMultiProcServer(handler, server_count = 3)
    grpc_server.start()
```