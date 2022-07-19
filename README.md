# Python SDK for Numaflow

This SDK provides the interface for writing [UDFs](https://numaproj.github.io/numaflow/user-defined-functions/) 
and [UDSinks](https://numaproj.github.io/numaflow/sinks/user-defined-sinks/) in Python.

## Implement a User Defined Function (UDF)

```python
from pynumaflow.function import Message, Messages, HTTPHandler
import random


def my_handler(key: bytes, value: bytes, _) -> Messages:
    messages = Messages()
    if random.randint(0, 10) % 2 == 0:
        messages.append(Message.to_all(value))
    else:
        messages.append(Message.to_drop())
    return messages


if __name__ == "__main__":
    handler = HTTPHandler(my_handler)
    handler.start()
```

### Sample Image

A sample UDF [Dockerfile](examples/function/udfproj/Dockerfile) is provided 
under [examples](examples/function/udfproj).


## Implement a User Defined Sink (UDSink)

```python
from typing import List
from pynumaflow.sink import Message, Responses, Response, HTTPSinkHandler


def udsink_handler(messages: List[Message], __) -> Responses:
    responses = Responses()
    for msg in messages:
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    handler = HTTPSinkHandler(udsink_handler)
    handler.start()
```

### Sample Image

A sample UDSink [Dockerfile](examples/sink/simplesink/Dockerfile) is provided 
under [examples](examples/sink/simplesink).
