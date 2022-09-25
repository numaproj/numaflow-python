# Python SDK for Numaflow

This SDK provides the interface for writing [UDFs](https://numaproj.github.io/numaflow/user-defined-functions/) 
and [UDSinks](https://numaproj.github.io/numaflow/sinks/user-defined-sinks/) in Python.

## Implement a User Defined Function (UDF)

```python

from pynumaflow.function import Messages, Message, Datum, UserDefinedFunctionServicer


def map_handler(key: str, datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    messages.append(Message.to_vtx(key, val))
    return messages


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(map_handler)
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
