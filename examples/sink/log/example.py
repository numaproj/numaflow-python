import os
from collections.abc import Iterator

from pynumaflow.sinker import Datum, Responses, Response, SinkServer
from pynumaflow.sinker import SinkerClass


class UserDefinedSink(SinkerClass):
    def handler(self, datums: Iterator[Datum]) -> Responses:
        responses = Responses()
        for msg in datums:
            print("User Defined Sink", msg.value.decode("utf-8"))
            responses.append(Response.as_success(msg.id))
        return responses


def udsink_handler(datums: Iterator[Datum]) -> Responses:
    responses = Responses()
    for msg in datums:
        print("User Defined Sink", msg.value.decode("utf-8"))
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    invoke = os.getenv("INVOKE", "handler")
    if invoke == "class":
        sink_handler = UserDefinedSink()
    else:
        sink_handler = udsink_handler
    grpc_server = SinkServer(sinker_instance=sink_handler)
    grpc_server.start()
