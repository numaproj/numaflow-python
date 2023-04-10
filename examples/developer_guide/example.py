import aiorun
from typing import Iterator, List

from pynumaflow.function import (
    Messages,
    Message,
    Datum,
    Metadata,
    UserDefinedFunctionServicer,
)


def map_handler(keys: List[str], datum: Datum) -> Messages:
    # forward a message
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    messages.append(Message.to_vtx(keys, val))
    return messages


async def my_handler(keys: List[str], datums: Iterator[Datum], md: Metadata) -> Messages:
    # count the number of events
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
    grpc_server = UserDefinedFunctionServicer(map_handler=map_handler, reduce_handler=my_handler)

    aiorun.run(grpc_server.start_async())
