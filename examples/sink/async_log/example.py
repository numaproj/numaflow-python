import os
from collections.abc import AsyncIterable


from pynumaflow.sinker import Datum, Responses, Response, Sinker
from pynumaflow.sinker import SinkAsyncServer
from pynumaflow._constants import _LOGGER


class UserDefinedSink(Sinker):
    async def handler(self, datums: AsyncIterable[Datum]) -> Responses:
        responses = Responses()
        async for msg in datums:
            _LOGGER.info("User Defined Sink %s", msg.value.decode("utf-8"))
            responses.append(Response.as_success(msg.id))
        return responses


async def udsink_handler(datums: AsyncIterable[Datum]) -> Responses:
    responses = Responses()
    async for msg in datums:
        _LOGGER.info("User Defined Sink %s", msg.value.decode("utf-8"))
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    invoke = os.getenv("INVOKE", "func_handler")
    if invoke == "class":
        sink_handler = UserDefinedSink()
    else:
        sink_handler = udsink_handler
    grpc_server = SinkAsyncServer(sink_handler)
    grpc_server.start()
