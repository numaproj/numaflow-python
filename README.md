# Python SDK for Numaflow

[![Build](https://github.com/numaproj/numaflow-python/actions/workflows/ci.yml/badge.svg)](https://github.com/numaproj/numaflow-python/actions/workflows/ci.yml)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Release Version](https://img.shields.io/github/v/release/numaproj/numaflow-python?label=pynumaflow)](https://github.com/numaproj/numaflow-python/releases/latest)


This SDK provides the interface for writing [UDFs](https://numaflow.numaproj.io/user-guide/user-defined-functions/user-defined-functions/)
and [UDSinks](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/) in Python.

## Installation

Install the package using pip.
```bash
pip install pynumaflow
```

### Build locally

This project uses [Poetry](https://python-poetry.org/) for dependency management and packaging.
To build the package locally, run the following command from the root of the project.

```bash
make setup
````

To run unit tests:
```bash
make test
```

To format code style using black and ruff:
```bash
make lint
```

Setup [pre-commit](https://pre-commit.com/) hooks:
```bash
pre-commit install
```

## Implement a User Defined Function (UDF)


### Map

```python
from pynumaflow.mapper import Messages, Message, Datum, MapServer


def handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    strs = val.decode("utf-8").split(",")
    messages = Messages()
    if len(strs) == 0:
        messages.append(Message.to_drop())
        return messages
    for s in strs:
        messages.append(Message(str.encode(s)))
    return messages


if __name__ == "__main__":
    grpc_server = MapServer(mapper_instance=handler)
    grpc_server.start()
```
### SourceTransformer - Map with event time assignment capability
In addition to the regular Map function, SourceTransformer supports assigning a new event time to the message.
SourceTransformer is only supported at source vertex to enable (a) early data filtering and (b) watermark assignment by extracting new event time from the message payload.

```python
from datetime import datetime
from pynumaflow.sourcetransformer import Messages, Message, Datum, SourceTransformServer


def transform_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    new_event_time = datetime.now()
    _ = datum.watermark
    message_t_s = Messages(Message(val, event_time=new_event_time, keys=keys))
    return message_t_s


if __name__ == "__main__":
    grpc_server = SourceTransformServer(source_transform_instance=transform_handler)
    grpc_server.start()
```

### Reduce

```python
from typing import AsyncIterable
from pynumaflow.reducer import Messages, Message, Datum, Metadata, ReduceAsyncServer


async def reduce_handler(keys: list[str], datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    return Messages(Message(str.encode(msg), keys=keys))


if __name__ == "__main__":
    grpc_server = ReduceAsyncServer(reducer_instance=reduce_handler)
    grpc_server.start()
```

### Sample Image
A sample UDF [Dockerfile](examples/map/forward_message/Dockerfile) is provided
under [examples](examples/map/forward_message).

## Implement a User Defined Sink (UDSink)

```python
from typing import Iterator
from pynumaflow.sinker import Datum, Responses, Response, SinkServer


def my_handler(datums: Iterator[Datum]) -> Responses:
    responses = Responses()
    for msg in datums:
        print("User Defined Sink", msg.value.decode("utf-8"))
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    grpc_server = SinkServer(sinker_instance=my_handler)
    grpc_server.start()
```

### Sample Image

A sample UDSink [Dockerfile](examples/sink/log/Dockerfile) is provided
under [examples](examples/sink/log).

## Class based handlers 

We can also implement UDFs and UDSinks using class based handlers.

The class based handlers are useful when we want to maintain state across multiple invocations of the handler.

Here we can pass the class instance to the server and the server will invoke the handler methods on the instance.

To use a class based handler, we the user needs to inherit the base class of the UDF/UDSink.
And implement the required methods in the class.

Example For Mapper, the user needs to inherit the [Mapper](pynumaflow/mapper/_dtypes.py#170) class and then implement the [handler](pynumaflow/mapper/_dtypes.py#170) method.

### Map

```python
from pynumaflow.mapper import Messages, Message, Datum, MapServer, Mapper


class MyHandler(Mapper):
    def handler(self, keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        strs = val.decode("utf-8").split(",")
        messages = Messages()
        if len(strs) == 0:
            messages.append(Message.to_drop())
            return messages
        for s in strs:
            messages.append(Message(str.encode(s)))
        return messages


if __name__ == "__main__":
    class_instance = MyHandler()
    grpc_server = MapServer(mapper_instance=class_instance)
    grpc_server.start()
```


## Server Types

For different types of UDFs and UDSinks, we have different server types which are supported.

These have different functionalities and are used for different use cases.

Currently we support the following server types:
1) SyncServer
2) AsyncServer
3) MultiProcessServer

Not all of the above are supported for all UDFs and UDSinks.



### SyncServer
```
grpc_server = MapServer(handler)
```

### AsyncServer
```
grpc_server = MapAsyncServer(handler)
```

### MultiProcessServer
```
grpc_server = MapMultiProcServer(handler)
```


