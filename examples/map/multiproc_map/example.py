import math

from pynumaflow._constants import ServerType

from pynumaflow.mapper import Messages, Message, Datum, MapServer, MapperClass


def is_prime(n):
    for i in range(2, int(math.ceil(math.sqrt(n)))):
        if n % i == 0:
            return False
    else:
        return True


class PrimeMap(MapperClass):
    def handler(self, keys: list[str], datum: Datum) -> Messages:
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
        NUM_CPU_MULTIPROC="N"
        Set the server_type = ServerType.Multiproc
    in the pipeline config for the numa container.
    """
    prime_class = PrimeMap()
    grpc_server = MapServer(mapper_instance=prime_class, server_type=ServerType.Multiproc)
    grpc_server.start()
