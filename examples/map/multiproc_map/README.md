# Multiprocessing Map

`pynumaflow` supports only asyncio based Reduce UDFs because we found that procedural Python is not able to handle 
any substantial traffic. 

This features enables the `pynumaflow` developer to utilise multiprocessing capabilities while 
writing UDFs using the map function. These are particularly useful for CPU intensive operations,
as it allows for better resource utilisation.

In this mode we would spawn N number (N = Cpu count) of grpc servers in different processes, where each of them are
listening on multiple TCP sockets.

To enable multiprocessing mode start the multiproc server in the UDF using the following command,
providing the optional argument `server_count` to specify the number of
servers to be forked (defaults to `os.cpu_count` if not provided):
```python
if __name__ == "__main__":
    grpc_server = MapMultiProcServer(handler, server_count = 3)
    grpc_server.start()
```