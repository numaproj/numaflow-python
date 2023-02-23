import asyncio
import json
from typing import AsyncIterable

import aiorun
import aiohttp
import time

from pynumaflow.function import Messages, Message, Datum, Metadata, UserDefinedFunctionServicer


# async def get_poke(session, pokemon_url):
#     async with session.get(pokemon_url) as resp:
#         pokemon = await resp.read()
#         try:
#             poke = json.loads(pokemon)
#             return poke['name']
#         except:
#             return "Error"

async def get_poke(session, pokemon_url):
    async with session.get(pokemon_url) as resp:
        pokemon = await resp.read()
        try:
            # return pokemon
            poke = json.loads(pokemon)
            return poke['message']
        except:
            return "Error"


async def reduce_handler(key: str, datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    async with aiohttp.ClientSession() as session:
        tasks = []
        async for _ in datums:
            # pokemon_url = f'https://pokeapi.co/api/v2/pokemon/{number}'
            pokemon_url = f'http://host.docker.internal:9888/ping'
            tasks.append(asyncio.ensure_future(get_poke(session, pokemon_url)))
        original_pokemon = await asyncio.gather(*tasks)

    msg = (
        f"counter:{original_pokemon} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    return Messages(Message.to_vtx(key, str.encode(msg)))


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(reduce_handler=reduce_handler)

    aiorun.run(grpc_server.start_async())
