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
        return poke["message"]
        # return poke['name']
    except:
        return "Error"


executor = concurrent.futures.ThreadPoolExecutor()


async def reduce_handler(key: str, datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    event_loop = asyncio.get_event_loop()
    tasks = []
    async for _ in datums:
        # pokemon_url = f'https://pokeapi.co/api/v2/pokemon/{number}'
        pokemon_url = f'http://host.docker.internal:9888/ping'
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
