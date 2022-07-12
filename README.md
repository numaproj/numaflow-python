# Python SDK

This SDK provides the interface of writing a UDF in Python.

## Implement UDF

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

## Build Image

A sample [Dockerfile](examples/function/udfproj/Dockerfile) is provided under [example](examples), run following command to build an image.

```shell
make image
```
