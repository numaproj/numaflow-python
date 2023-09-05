from collections.abc import AsyncIterable

import aiorun

from pynumaflow.sinker import Datum, Responses, Response, AsyncSinker


async def udsink_handler(datums: AsyncIterable[Datum]) -> Responses:
    responses = Responses()
    async for msg in datums:
        print("User Defined Sink", msg.value.decode("utf-8"))
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    grpc_server = AsyncSinker(handler=udsink_handler)
    aiorun.run(grpc_server.start())
