from pynumaflow.function import Messages, Message, Datum, Server


def my_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    messages.append(Message(val, keys=keys))
    return messages


if __name__ == "__main__":
    grpc_server = Server(map_handler=my_handler)
    grpc_server.start()
