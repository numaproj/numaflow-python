# Python SDK for Numaflow

This SDK provides the interface for writing [UDFs](https://numaflow.numaproj.io/user-guide/user-defined-functions/user-defined-functions/) 
and [UDSinks](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/) in Python.

## Implement a User Defined Function (UDF)


### Map

```python
from pynumaflow.function import Messages, Message, Datum, Server
from typing import List


def my_handler(keys: List[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages(Message(value=val, keys=keys))
    return messages


if __name__ == "__main__":
    grpc_server = Server(map_handler=my_handler)
    grpc_server.start()
```
### MapT - Map with event time assignment capability
In addition to the regular Map function, MapT supports assigning a new event time to the message.
MapT is only supported at source vertex to enable (a) early data filtering and (b) watermark assignment by extracting new event time from the message payload.

```python
import datetime
from pynumaflow.function import MessageTs, MessageT, Datum, Server
from typing import List


def mapt_handler(keys: List[str], datum: Datum) -> MessageTs:
    val = datum.value
    new_event_time = datetime.time()
    _ = datum.watermark
    message_t_s = MessageTs(MessageT(new_event_time, val, keys))
    return message_t_s


if __name__ == "__main__":
    grpc_server = Server(mapt_handler=mapt_handler)
    grpc_server.start()
```

### Reduce

```python
import aiorun
import asyncio
from typing import Iterator, List
from pynumaflow.function import Messages, Message, Datum, Metadata, AsyncServer


async def my_handler(keys: List[str], datums: Iterator[Datum], md: Metadata) -> Messages:
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
def my_handler(keys: List[str], datum: Datum) -> Messages:
    num_delivered = datum.metadata.num_delivered
    # Choose to do specific actions, if the message delivery count reaches a certain threshold.
    if num_delivered > 3:
        ...
```