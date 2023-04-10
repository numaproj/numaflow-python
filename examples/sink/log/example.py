from typing import Iterator

from pynumaflow.sink import Datum, Responses, Response, UserDefinedSinkServicer


def udsink_handler(datums: Iterator[Datum]) -> Responses:
    responses = Responses()
    for msg in datums:
        print("User Defined Sink", msg.value.decode("utf-8"))
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    grpc_server = UserDefinedSinkServicer(sink_handler=udsink_handler)
    grpc_server.start()
