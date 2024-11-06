import aiorun
import grpc

from pynumaflow.info.types import (
    ServerInfo,
    MAP_MODE_KEY,
    MapMode,
    MINIMUM_NUMAFLOW_VERSION,
    ContainerType,
)
from pynumaflow.mapstreamer.servicer.async_servicer import AsyncMapStreamServicer
from pynumaflow.proto.mapper import map_pb2_grpc

from pynumaflow._constants import (
    MAP_STREAM_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    NUM_THREADS_DEFAULT,
    _LOGGER,
    MAP_SERVER_INFO_FILE_PATH,
    MAX_NUM_THREADS,
)

from pynumaflow.mapstreamer._dtypes import MapStreamCallable

from pynumaflow.shared.server import NumaflowServer, start_async_server


class MapStreamAsyncServer(NumaflowServer):
    """
    Class for a new Map Stream Server instance.
    """

    def __init__(
        self,
        map_stream_instance: MapStreamCallable,
        sock_path=MAP_STREAM_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=MAP_SERVER_INFO_FILE_PATH,
    ):
        """
        Create a new grpc Async Map Stream Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
            map_stream_instance: The map stream instance to be used for Map Stream UDF
            sock_path: The UNIX socket path to be used for the server
            max_message_size: The max message size in bytes the server can receive and send
            max_threads: The max number of threads to be spawned;
                            defaults to 4 and max capped at 16
            server_type: The type of server to be used

        Example invocation:
            import os
            from collections.abc import AsyncIterable
            from pynumaflow.mapstreamer import Message, Datum, MapStreamAsyncServer, MapStreamer

            class FlatMapStream(MapStreamer):
                async def handler(self, keys: list[str], datum: Datum) -> AsyncIterable[Message]:
                    val = datum.value
                    _ = datum.event_time
                    _ = datum.watermark
                    strs = val.decode("utf-8").split(",")

                    if len(strs) == 0:
                        yield Message.to_drop()
                        return
                    for s in strs:
                        yield Message(str.encode(s))

            async def map_stream_handler(_: list[str], datum: Datum) -> AsyncIterable[Message]:

                val = datum.value
                _ = datum.event_time
                _ = datum.watermark
                strs = val.decode("utf-8").split(",")

                if len(strs) == 0:
                    yield Message.to_drop()
                    return
                for s in strs:
                    yield Message(str.encode(s))

            if __name__ == "__main__":
                invoke = os.getenv("INVOKE", "func_handler")
                if invoke == "class":
                    handler = FlatMapStream()
                else:
                    handler = map_stream_handler
                grpc_server = MapStreamAsyncServer(handler)
                grpc_server.start()

        """
        self.map_stream_instance: MapStreamCallable = map_stream_instance
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.servicer = AsyncMapStreamServicer(handler=self.map_stream_instance)

    def start(self):
        """
        Starter function for the Async Map Stream server, we need a separate caller
        to the aexec so that all the async coroutines can be started from a single context
        """
        aiorun.run(self.aexec(), use_uvloop=True)

    async def aexec(self):
        """
        Starts the Async gRPC server on the given UNIX socket with
        given max threads.
        """
        # As the server is async, we need to create a new server instance in the
        # same thread as the event loop so that all the async calls are made in the
        # same context
        # Create a new async server instance and add the servicer to it
        server = grpc.aio.server(options=self._server_options)
        server.add_insecure_port(self.sock_path)
        map_pb2_grpc.add_MapServicer_to_server(
            self.servicer,
            server,
        )
        _LOGGER.info("Starting Map Stream Server")
        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Mapper]
        # Add the MAP_MODE metadata to the server info for the correct map mode
        serv_info.metadata[MAP_MODE_KEY] = MapMode.StreamMap

        # Start the async server
        await start_async_server(
            server_async=server,
            sock_path=self.sock_path,
            max_threads=self.max_threads,
            cleanup_coroutines=list(),
            server_info_file=self.server_info_file,
            server_info=serv_info,
        )
