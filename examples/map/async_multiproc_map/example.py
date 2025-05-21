import os

from pynumaflow.mapper import Messages, Message, Datum, Mapper, AsyncMapMultiprocServer
from pynumaflow._constants import _LOGGER


class FlatMap(Mapper):
    """
    This class needs to be of type Mapper class to be used
    as a handler for the MapServer class.
    Example of a mapper that calculates if a number is prime.
    """

    async def handler(self, keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        messages = Messages()
        messages.append(Message(val, keys=keys))
        _LOGGER.info(f"MY PID {os.getpid()}")
        return messages


if __name__ == "__main__":
    """
    Example of starting a multiprocessing map vertex.
    """
    # To set the env server_count value set the env variable
    # NUM_CPU_MULTIPROC="N"
    server_count = int(os.getenv("NUM_CPU_MULTIPROC", "2"))
    server_type = int(os.getenv("SERVER_KIND", "tcp"))
    use_tcp = False
    if server_type == "tcp":
        use_tcp = True
    elif server_type == "uds":
        use_tcp = False
    _class = FlatMap()
    # Server count is the number of server processes to start
    grpc_server = AsyncMapMultiprocServer(_class, server_count=server_count, use_tcp=use_tcp)
    grpc_server.start()
