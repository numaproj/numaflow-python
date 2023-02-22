import asyncio
from typing import AsyncIterable

import aiorun
import aiohttp
import time
import requests

from pynumaflow.function import Messages, Message, Datum, Metadata, UserDefinedFunctionServicer


def blocks(n):
    # log = logging.getLogger('blocks({})'.format(n))
    # log.info('running')
    fact = 1
    for i in range(1, n + 1):
        fact = fact * i
    time.sleep(0.1)
    # log.info('done')
    return fact


def blocking(url):
    res = requests.get(url)
    try:
        poke = res.json()
        return poke["message"]
        # return poke['name']
    except:
        return "Error"


async def reduce_handler(key: str, datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    tasks = []
    for number in range(1, 20):
        # pokemon_url = f'https://pokeapi.co/api/v2/pokemon/{number}'
        pokemon_url = f'http://host.docker.internal:9888/ping'
        tasks.append(blocking(pokemon_url))
    msg = (
        f"counter:{tasks} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    return Messages(Message.to_vtx(key, str.encode(msg)))


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(reduce_handler=reduce_handler, sock_path="/tmp/numaflow-test.sock")

    aiorun.run(grpc_server.start_async())
