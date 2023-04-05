# Python SDK for Numaflow

This SDK provides the interface for writing [UDFs](https://numaflow.numaproj.io/user-guide/user-defined-functions/user-defined-functions/) 
and [UDSinks](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/) in Python.

## Implement a User Defined Function (UDF)


### Map

```python
from pynumaflow.function import Messages, Message, Datum, UserDefinedFunctionServicer


def my_handler(keys: List[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages(Message.to_vtx(keys, val))
    return messages


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(map_handler=my_handler)
    grpc_server.start()
```
### MapT - Map with event time assignment capability
In addition to the regular Map function, MapT supports assigning a new event time to the message.
MapT is only supported at source vertex to enable (a) early data filtering and (b) watermark assignment by extracting new event time from the message payload.

```python
import datetime
from pynumaflow.function import MessageTs, MessageT, Datum, UserDefinedFunctionServicer

def mapt_handler(keys: List[str], datum: Datum) -> MessageTs:
    val = datum.value
    new_event_time = datetime.time()
    _ = datum.watermark
    message_t_s = MessageTs(MessageT.to_vtx(keys, val, new_event_time))
    return message_t_s

if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(mapt_handler=mapt_handler)
    grpc_server.start()
```

### Reduce

```python
import asyncio
from typing import Iterator
from pynumaflow.function import Messages, Message, Datum, Metadata, UserDefinedFunctionServicer


async def my_handler(keys: List[str], datums: Iterator[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    return Messages(Message.to_vtx(keys, str.encode(msg)))


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(reduce_handler=my_handler)
    asyncio.run(grpc_server.start())
    asyncio.run(*grpc_server.cleanup_coroutines)
```

### Sample Image
A sample UDF [Dockerfile](examples/function/forward_message/Dockerfile) is provided 
under [examples](examples/function/forward_message).

## Implement a User Defined Sink (UDSink)

```python
from typing import Iterator
from pynumaflow.sink import Datum, Responses, Response, UserDefinedSinkServicer


def my_handler(datums: Iterator[Datum]) -> Responses:
    responses = Responses()
    for msg in datums:
        print("User Defined Sink", msg.value.decode("utf-8"))
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    grpc_server = UserDefinedSinkServicer(my_handler)
    grpc_server.start()
```

### Sample Image

A sample UDSink [Dockerfile](examples/sink/log/Dockerfile) is provided 
under [examples](examples/sink/log).
