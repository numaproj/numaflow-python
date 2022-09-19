from typing import List
from pynumaflow.sink import Datum, Responses, Response, UserDefinedSinkServicer


def udsink_handler(datums: List[Datum], __) -> Responses:
    responses = Responses()
    for msg in datums:
        print("User Defined Sink", msg)
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    grpc_server = UserDefinedSinkServicer(udsink_handler)
    grpc_server.start()
