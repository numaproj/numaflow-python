# Multiprocessing Map

`pynumaflow` supports only asyncio based Reduce UDFs because we found that the procedural Python is not able to handle 
any substantial traffic.

This features enables the `pynumaflow` developer to utilise multiprocessing capabilities while 
writing UDFs using map function. These are particularly useful for CPU intensive operations,
as it allows for better resource utilisation.

In this mode we would spawn N number (N = Cpu count) of grpc servers in different processes, where each of them
listening on the same TCP socket. 

This is possible by enabling the `SO_REUSEPORT` flag for the TCP socket, which allows these different
processes to bind to the same port. 

To enable multiprocessing mode 

1) Start the multiproc server in the UDF using the following command 
```python
if __name__ == "__main__":
    grpc_server = MultiProcServer(map_handler=my_handler)
    grpc_server.start()
```
2) Set the ENV var value `NUM_CPU_MULTIPROC="n"` for the UDF container,
to set the value of the number of server instances (one for each subprocess) to be created.