import math

from pynumaflow.mapper import Messages, Message, Datum, MultiProcMapper


def is_prime(n):
    for i in range(2, int(math.ceil(math.sqrt(n)))):
        if n % i == 0:
            return False
    else:
        return True


def my_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    for i in range(2, 100000):
        is_prime(i)
    messages.append(Message(val, keys=keys))
    return messages


if __name__ == "__main__":
    """
    Example of starting a multiprocessing map vertex.
    To enable set the env variable
        MAP_MULTIPROC="true"
    in the pipeline config for the numa container.
    """
    grpc_server = MultiProcMapper(map_handler=my_handler)
    grpc_server.start()
