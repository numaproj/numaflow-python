"""
Write a class which implements the MapperClass interface.
"""
from pynumaflow.mapper import ServerType
from pynumaflow.mapper import Datum, Messages, Message
from pynumaflow.mapper.map import MapServer
from pynumaflow.mapper._dtypes import MapperClass


class ExampleMapperClass(MapperClass):
    """
    Provides an interface to write a Mapper
    """

    def handler(self, keys: [str], datum: Datum) -> Messages:
        """
        Write a handler function which implements the MapCallable interface.
        """
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        messages = Messages(Message(val, keys=keys))
        return messages


def handler_new(keys: [str], datum: Datum) -> Messages:
    """
    Write a handler function which implements the MapCallable interface.
    """
    val = datum.value
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages(Message(val, keys=keys))
    return messages


# Write a main function to create a new MapServer instance.
if __name__ == "__main__":
    map_instance = ExampleMapperClass()
    grpc_server = MapServer(mapper_instance=map_instance, server_type=ServerType.Async)
    grpc_server.start()
