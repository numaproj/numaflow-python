import asyncio
import json
from typing import AsyncIterable

import aiorun
import aiohttp
from pynumaflow import setup_logging
import time

from pynumaflow.function import Messages, Message, Datum, Metadata, UserDefinedFunctionServicer

_LOGGER = setup_logging(__name__)


async def http_request(session, url):
    async with session.get(url) as resp:
        result = await resp.read()
        try:
            res_json = json.loads(result)
            return res_json['message']
        except Exception as e:
            _LOGGER.error("HTTP request error: %s", e)
            return "Error"


async def reduce_handler(key: str, datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    async with aiohttp.ClientSession() as session:
        tasks = []
        start_time = time.time()
        async for _ in datums:
            url = f'http://host.docker.internal:9888/ping'
            tasks.append(http_request(session, url))
        co_time = time.time()
        results = await asyncio.gather(*tasks)
        end_time = time.time()

    msg = (
        # f"loop_time:{co_time-start_time} batch_time:{end_time-start_time} co_time:{end_time-co_time} interval_window_start:{interval_window.start} "
        # f"interval_window_end:{interval_window.end}"
        f"batch_time:{end_time - start_time}, interval_window_start:{interval_window.start}"
    )
    return Messages(Message.to_vtx(key, str.encode(msg)))


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(reduce_handler=reduce_handler)
    aiorun.run(grpc_server.start_async())
