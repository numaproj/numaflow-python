import os
from collections.abc import AsyncIterable


from pynumaflow.sinker import Datum, Responses, Response, SinkServer, SinkerClass, ServerType


class UserDefinedSink(SinkerClass):
    async def handler(self, datums: AsyncIterable[Datum]) -> Responses:
        responses = Responses()
        async for msg in datums:
            print("User Defined Sink", msg.value.decode("utf-8"))
            responses.append(Response.as_success(msg.id))
        return responses


async def udsink_handler(datums: AsyncIterable[Datum]) -> Responses:
    responses = Responses()
    async for msg in datums:
        print("User Defined Sink", msg.value.decode("utf-8"))
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    invoke = os.getenv("INVOKE", "handler")
    if invoke == "class":
        sink_handler = UserDefinedSink()
    else:
        sink_handler = udsink_handler
    grpc_server = SinkServer(sinker_instance=sink_handler, server_type=ServerType.Async)
    grpc_server.start()
