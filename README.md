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
from pynumaflow.function import Messages, Message, Datum, Server


def my_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    return Messages(Message(value=val, keys=keys))


if __name__ == "__main__":
    grpc_server = Server(map_handler=my_handler)
    grpc_server.start()
```
### MapT - Map with event time assignment capability
In addition to the regular Map function, MapT supports assigning a new event time to the message.
MapT is only supported at source vertex to enable (a) early data filtering and (b) watermark assignment by extracting new event time from the message payload.

```python
from datetime import datetime
from pynumaflow.function import MessageTs, MessageT, Datum, Server


def mapt_handler(keys: list[str], datum: Datum) -> MessageTs:
    val = datum.value
    new_event_time = datetime.now()
    _ = datum.watermark
    message_t_s = MessageTs(MessageT(val, event_time=new_event_time, keys=keys))
    return message_t_s


if __name__ == "__main__":
    grpc_server = Server(mapt_handler=mapt_handler)
    grpc_server.start()
```

### Reduce

```python
import aiorun
from typing import Iterator, List
from pynumaflow.function import Messages, Message, Datum, Metadata, AsyncServer


async def my_handler(
    keys: List[str], datums: Iterator[Datum], md: Metadata
) -> Messages:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    return Messages(Message(str.encode(msg), keys))


if __name__ == "__main__":
    grpc_server = AsyncServer(reduce_handler=my_handler)
    aiorun.run(grpc_server.start())
```

### Sample Image
A sample UDF [Dockerfile](examples/function/forward_message/Dockerfile) is provided
under [examples](examples/function/forward_message).

## Implement a User Defined Sink (UDSink)

```python
from typing import Iterator
from pynumaflow.sink import Datum, Responses, Response, Sink


def my_handler(datums: Iterator[Datum]) -> Responses:
    responses = Responses()
    for msg in datums:
        print("User Defined Sink", msg.value.decode("utf-8"))
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    grpc_server = Sink(my_handler)
    grpc_server.start()
```

### Sample Image

A sample UDSink [Dockerfile](examples/sink/log/Dockerfile) is provided
under [examples](examples/sink/log).

### Datum Metadata
The Datum object contains the message payload and metadata. Currently, there are two fields
in metadata: the message ID, the message delivery count to indicate how many times the message
has been delivered. You can use these metadata to implement customized logic. For example,
```python
...


def my_handler(keys: list[str], datum: Datum) -> Messages:
    num_delivered = datum.metadata.num_delivered
    # Choose to do specific actions, if the message delivery count reaches a certain threshold.
    if num_delivered > 3:
        ...
```
