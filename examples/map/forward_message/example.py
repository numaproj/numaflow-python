from pynumaflow.mapper import Messages, Message, Datum, MapServer


def my_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    messages.append(Message(value=val, keys=keys))
    return messages


if __name__ == "__main__":
    grpc_server = MapServer(mapper_instance=my_handler)
    grpc_server.start()
