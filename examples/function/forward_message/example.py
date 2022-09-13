from pynumaflow.function import Messages, Message, Datum
from pynumaflow.function.server import UserDefinedFunctionServicer


def map_handler(key: str, datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    messages.append(Message.to_vtx(key, val))
    return messages


if __name__ == "__main__":
    grpc_server = UserDefinedFunctionServicer(map_handler)
    grpc_server.start()
