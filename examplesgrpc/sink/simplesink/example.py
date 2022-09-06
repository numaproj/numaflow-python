from typing import List

from pynumaflowgrpc.sink import Message, Responses, Response, HTTPSinkHandler


def udsink_handler(messages: List[Message], __) -> Responses:
    responses = Responses()
    for msg in messages:
        print("Msg", msg)
        responses.append(Response.as_success(msg.id))
    return responses


if __name__ == "__main__":
    handler = HTTPSinkHandler(udsink_handler)
    handler.start()
