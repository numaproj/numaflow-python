import asyncio
from collections.abc import AsyncIterable, Iterator, Coroutine
from typing import Any

from pynumaflow.function import (
    Messages,
    Message,
    Datum,
    Metadata,
    UserDefinedFunctionServicer,
)


class UserDefinedFunction:

    def __init__(self):
        pass

    def map_handler(self, key: str, datum: Datum) -> Messages:
        # forward a message
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        messages = Messages()
        messages.append(Message.to_vtx(key, val))
        return messages

    async def my_handler(self, key: str, datums: Iterator[Datum], md: Metadata):
        # count the number of events
        interval_window = md.interval_window
        counter = 0
        async for _ in datums:
            counter += 1

        msg = (
            f"counter:{counter} interval_window_start:{interval_window.start} "
            f"interval_window_end:{interval_window.end}"
        )
        return Messages(Message.to_vtx(key, str.encode(msg)))

    def reduce_cb(self, key: str, datums: Iterator[Datum], md: Metadata):
        return asyncio.create_task(self.my_handler(key, datums, md))


if __name__ == "__main__":
    udf = UserDefinedFunction()
    grpc_server = UserDefinedFunctionServicer(
        map_handler=udf.map_handler, reduce_handler=udf.my_handler
    )

    asyncio.run(grpc_server.start_async())
    asyncio.run(grpc_server.cleanup_coroutines)
