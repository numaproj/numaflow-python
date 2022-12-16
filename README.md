# Python SDK for Numaflow

This SDK provides the interface for writing [UDFs](https://numaflow.numaproj.io/user-guide/user-defined-functions/) 
and [UDSinks](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/) in Python.

## Implement a User Defined Function (UDF)

```python
from typing import Iterator
from pynumaflow.function import Messages, Message, Datum, Metadata, UserDefinedFunctionServicer

def map_handler(key: str, datum: Datum) -> Messages:
      val = datum.value
      _ = datum.event_time
      _ = datum.watermark
      messages = Messages(Message.to_vtx(key, val))
      return messages

def reduce_handler(key: str, datums: Iterator[Datum], md: Metadata) -> Messages:
      interval_window = md.interval_window
      print("Interval window:", interval_window)
      counter = 0
      for datum in datums:
        print("datum:", datum)
        counter = counter + 1
      messages = Messages()
      messages.append(Message.to_vtx(key, str.encode(str(counter))))
      return messages


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(map_handler=map_handler, reduce_handler=reduce_handler)
    grpc_server.start()
```

### Sample Image (TODO)

## Implement a User Defined Sink (UDSink)

```python
from typing import List
from pynumaflow.sink import Datum, Responses, Response, UserDefinedSinkServicer


def udsink_handler(datums: List[Datum]) -> Responses:
    responses = Responses()
    for msg in datums:
        print("User Defined Sink", msg)
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    grpc_server = UserDefinedSinkServicer(udsink_handler)
    grpc_server.start()
```

### Sample Image

A sample UDSink [Dockerfile](examples/sink/simplesink/Dockerfile) is provided 
under [examples](examples/sink/simplesink).
