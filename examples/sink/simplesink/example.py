from typing import List
from pynumaflow.sink import Datum, Responses, Response, UserDefinedSinkServicer


def udsink_handler(datums: List[Datum]) -> Responses:
    responses = Responses()
    for msg in datums:
        if "err" in msg.value.decode("utf-8"):
            responses.append(Response.as_failure(msg.id, "mock sink message error"))
        else:
            print("User Defined Sink", msg)
            responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    grpc_server = UserDefinedSinkServicer(udsink_handler)
    grpc_server.start()
