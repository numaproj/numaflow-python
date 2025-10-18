import math
import os

from pynumaflow.mapper import Messages, Message, Datum, Mapper, MapMultiprocServer


def is_prime(n):
    for i in range(2, int(math.ceil(math.sqrt(n)))):
        if n % i == 0:
            return False
    else:
        return True


class PrimeMap(Mapper):
    """
    This class needs to be of type Mapper class to be used
    as a handler for the MapServer class.
    Example of a mapper that calculates if a number is prime.
    """

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
    """
    # To set the env server_count value set the env variable
    # NUM_CPU_MULTIPROC="N"
    server_count = int(os.getenv("NUM_CPU_MULTIPROC", "2"))
    prime_class = PrimeMap()
    # Server count is the number of server processes to start
    grpc_server = MapMultiprocServer(prime_class, server_count=server_count)
    grpc_server.start()
