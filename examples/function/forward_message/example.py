from pynumaflow.function import Messages, Message, Datum
from pynumaflow.function.server import UserDefinedFunctionServicer


def map_handler(key: str, datum: Datum) -> Messages:
    val = datum.value()
    _ = datum.event_time()
    _ = datum.water_mark()
    messages = Messages()
    messages.append(Message.to_vtx(key, val))
    return messages


if __name__ == "__main__":
    grpcServer = UserDefinedFunctionServicer(map_handler, 'unix:///tmp/numaflow-test.sock')
    grpcServer.start()
