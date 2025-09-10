# Reducer in Python

For creating a reducer UDF we can use two different approaches:
- Class based reducer
  - For the class based reducer we need to implement a class that inherits from the `Reducer` class and implements the required methods.
  - Next we need to create a `ReduceAsyncServer` instance and pass the reducer class to it along with any input args or 
  kwargs that the custom reducer class requires.
  - Finally we need to call the `start` method on the `ReduceAsyncServer` instance to start the reducer server.
  ```python
  from collections.abc import AsyncIterable
  from pynumaflow.reducer import Reducer, ReduceAsyncServer, Datum, Message, Messages, Metadata

  class Example(Reducer):
      def __init__(self, counter):
          self.counter = counter
  
      async def handler(
          self, keys: list[str], datums: AsyncIterable[Datum], md: Metadata
      ) -> Messages:
          interval_window = md.interval_window
          self.counter = 0
          async for _ in datums:
              self.counter += 1
          msg = (
              f"counter:{self.counter} interval_window_start:{interval_window.start} "
              f"interval_window_end:{interval_window.end}"
          )
          return Messages(Message(str.encode(msg), keys=keys))

  if __name__ == "__main__":
      # Here we are using the class instance as the reducer_instance
      # which will be used to invoke the handler function.
      # We are passing the init_args for the class instance.
      grpc_server = ReduceAsyncServer(Example, init_args=(0,))
      grpc_server.start()
  ``` 

- Function based reducer
    For the function based reducer we need to create a function of the signature
    ```python
    async def handler(keys: list[str], datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    ```
    that takes the required arguments and returns the `Messages` object.
    - Next we need to create a `ReduceAsyncServer` instance and pass the function to it along with any input args or kwargs that the custom reducer function requires.
    - Finally we need to call the `start` method on the `ReduceAsyncServer` instance to start the reducer server.
    - We must ensure that no init_args or init_kwargs are passed to the `ReduceAsyncServer` instance as they are not used for function based reducers.
    ```python
        from numaflow import ReduceAsyncServer
        async def handler(keys: list[str], datums: AsyncIterable[Datum], md: Metadata) -> Messages:
            counter = 0
            interval_window = md.interval_window
            async for _ in datums:
                counter += 1
            msg = (
                f"counter:{counter} interval_window_start:{interval_window.start} "
                f"interval_window_end:{interval_window.end}"
            )
            return Messages(Message(str.encode(msg), keys=keys))
    
        if __name__ == "__main__":
        # Here we are using the function as the reducer_instance
        # which will be used to invoke the handler function.
        grpc_server = ReduceAsyncServer(handler)
        grpc_server.start()
    ```


