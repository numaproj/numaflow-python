import os

from pynumaflowgrpc.function import HTTPHandler
from pynumaflowgrpc.function import Messages, Message
from pynumaflowgrpc.server import UserDefinedFunctionServicer


def map_handler(key: str, value: bytes) -> Messages:
    messages = Messages()
    messages.append(Message.to_vtx(key, value))
    return messages


if __name__ == "__main__":
    grpcServer = UserDefinedFunctionServicer(map_handler)
    grpcServer.start()
