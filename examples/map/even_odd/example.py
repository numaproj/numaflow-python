from pynumaflow.mapper import Messages, Message, Datum, MapServer


def my_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    output_keys = keys
    output_tags = []
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    num = int.from_bytes(val, "little")

    if num % 2 == 0:
        output_keys = ["even"]
        output_tags = ["even-tag"]
    else:
        output_keys = ["odd"]
        output_tags = ["odd-tag"]

    messages.append(Message(val, keys=output_keys, tags=output_tags))
    return messages


if __name__ == "__main__":
    """
    This example shows how to create a simple map function that takes in a
    number and outputs it to the "even" or "odd" key depending on whether it
    is even or odd.
    We use a function as handler, but a class that implements
    a Mapper can be used as well.
    """
    grpc_server = MapServer(my_handler)
    grpc_server.start()
