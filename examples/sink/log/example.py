from collections.abc import Iterator

from pynumaflow.sinker import Datum, Responses, Response, Sinker


def udsink_handler(datums: Iterator[Datum]) -> Responses:
    responses = Responses()
    for msg in datums:
        print("User Defined Sink", msg.value.decode("utf-8"))
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    grpc_server = Sinker(handler=udsink_handler)
    grpc_server.start()
