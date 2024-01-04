from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
import grpc
from pynumaflow._constants import MAX_MESSAGE_SIZE, MAX_THREADS, ServerType, _LOGGER
from pynumaflow.info.server import get_sdk_version, write as info_server_write
from pynumaflow.info.types import ServerInfo, Protocol, Language, SERVER_INFO_FILE_PATH


class NumaflowServer:
    """
    Provides an interface to write a Numaflow Server
    which will be exposed over gRPC.

    Members:
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4
    """

    @abstractmethod
    def start(self):
        """
        Start the server
        """
        raise NotImplementedError


def prepare_server(sock_path: str,
                   server_type: ServerType,
                   max_message_size=MAX_MESSAGE_SIZE,
                   max_threads=MAX_THREADS,
                   ):
    """
        Create a new grpc Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.

    """
    _server_options = [
        ("grpc.max_send_message_length", max_message_size),
        ("grpc.max_receive_message_length", max_message_size),
    ]
    server = grpc.server(
            ThreadPoolExecutor(max_workers=max_threads), options=_server_options)
    if server_type == ServerType.Async:
        server = grpc.aio.server(options=_server_options)
    server.add_insecure_port(sock_path)
    return server


def write_info_file(protocol: Protocol) -> None:
    """
    Write the server info file to the given path.
    """
    serv_info = ServerInfo(
        protocol=protocol,
        language=Language.PYTHON,
        version=get_sdk_version(),
    )
    info_server_write(server_info=serv_info, info_file=SERVER_INFO_FILE_PATH)


async def __serve_async(self, server) -> None:

    async def server_graceful_shutdown():
        """
        Shuts down the server with 5 seconds of grace period. During the
        grace period, the server won't accept new connections and allow
        existing RPCs to continue within the grace period.
        """
        _LOGGER.info("Starting graceful shutdown...")
        await server.stop(5)

    self.cleanup_coroutines.append(server_graceful_shutdown())
    await server.wait_for_termination()

async def start(self) -> None:
    """Starts the Async gRPC mapper on the given UNIX socket."""
    server = grpc.aio.server(options=self._server_options)
    await self.__serve_async(server)