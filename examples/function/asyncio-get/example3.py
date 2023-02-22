import asyncio
import concurrent.futures
from typing import AsyncIterable

import aiorun
import requests

from pynumaflow.function import Messages, Message, Datum, Metadata, UserDefinedFunctionServicer


def blocking(url):
    res = requests.get(url)
    try:
        poke = res.json()
        return poke['name']
    except:
        return "Error"


async def reduce_handler(key: str, datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    executor = concurrent.futures.ThreadPoolExecutor()
    event_loop = asyncio.get_event_loop()
    tasks = []
    for number in range(1, 10):
        pokemon_url = f'https://pokeapi.co/api/v2/pokemon/{number}'
        fut = event_loop.run_in_executor(executor, blocking, pokemon_url)
        tasks.append(fut)
    completed, pending = await asyncio.wait(tasks)
    results = [t.result() for t in completed]
    msg = (
        f"counter:{results} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    return Messages(Message.to_vtx(key, str.encode(msg)))


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(reduce_handler=reduce_handler)

    aiorun.run(grpc_server.start_async())
