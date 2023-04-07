import math

from pynumaflow.function import Messages, Message, Datum, UserDefinedFunctionServicer


def is_prime(n):
    for i in range(2, int(math.ceil(math.sqrt(n)))):
        if n % i == 0:
            return False
    else:
        return True


def my_handler(key: str, datum: Datum) -> Messages:
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    for i in range(2, 100000):
        is_prime(i)
    messages.append(Message.to_vtx(key, val))
    return messages


if __name__ == "__main__":
    """
    Example of starting a multiprocessing map vertex.
    To enable set the env variable
        MAP_MULTIPROC="true"
    in the pipeline config for the numa container.
    """
    grpc_server = UserDefinedFunctionServicer(map_handler=my_handler)
    grpc_server.start_multiproc()
