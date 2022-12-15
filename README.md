# Python SDK for Numaflow

This SDK provides the interface for writing [UDFs](https://numaflow.numaproj.io/user-guide/user-defined-functions/) 
and [UDSinks](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/) in Python.

## Implement a User Defined Function (UDF)

```python

from pynumaflow.function import Messages, Message, Datum, UserDefinedFunctionServicer


def function_handler(key: str, datum: Datum) -> Messages:
    """
    Simple UDF that relays an incoming message.
    """
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages(Message(key=key, value=val))
    return messages


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(function_handler)
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
