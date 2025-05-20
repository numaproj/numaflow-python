import logging
import multiprocessing

import aiorun
import grpc

from pynumaflow._constants import (
    MAX_NUM_THREADS,
    MAX_MESSAGE_SIZE,
    MAP_SERVER_INFO_FILE_PATH,
    _PROCESS_COUNT,
    NUM_THREADS_DEFAULT,
    MULTIPROC_MAP_SOCK_ADDR,
)
from pynumaflow.info.server import get_metadata_env
from pynumaflow.info.types import (
    ServerInfo,
    MINIMUM_NUMAFLOW_VERSION,
    ContainerType,
    MAP_MODE_KEY,
    MapMode,
    METADATA_ENVS,
    MULTIPROC_KEY,
    MULTIPROC_ENDPOINTS,
    Protocol,
)
from pynumaflow.mapper._dtypes import MapAsyncCallable
from pynumaflow.mapper._servicer._async_servicer import AsyncMapServicer
from pynumaflow.proto.mapper import map_pb2_grpc
from pynumaflow.shared.server import start_async_server, NumaflowServer, reserve_port
from pynumaflow.info.server import write as info_server_write

_LOGGER = logging.getLogger(__name__)


class AsyncMultiprocMapServer(NumaflowServer):
    """
    A multiprocess asynchronous gRPC server for Numaflow Map UDFs.
    Spawns N worker processes, each running an asyncio-based gRPC server.
    """

    def __init__(
        self,
        mapper_instance: MapAsyncCallable,
        server_count: int = _PROCESS_COUNT,
        sock_path: str = MULTIPROC_MAP_SOCK_ADDR,
        max_message_size: int = MAX_MESSAGE_SIZE,
        max_threads: int = NUM_THREADS_DEFAULT,
        server_info_file: str = MAP_SERVER_INFO_FILE_PATH,
        use_tcp: bool = False,
    ):
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file
        self.use_tcp = use_tcp

        self.mapper_instance = mapper_instance

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
            ("grpc.so_reuseport", 1),
            ("grpc.so_reuseaddr", 1),
        ]

        self._process_count = min(server_count, 2 * _PROCESS_COUNT)
        self.servicer = AsyncMapServicer(handler=self.mapper_instance, multiproc=True)

    def start(self):
        """
        Starts the multiprocess async gRPC servers.
        """
        _LOGGER.info("Starting async multiprocess gRPC server with %d workers", self._process_count)

        workers = []
        ports = []

        for idx in range(self._process_count):
            if self.use_tcp:
                with reserve_port(0) as reserved_port:
                    bind_address = f"0.0.0.0:{reserved_port}"
                    ports.append(f"http://{bind_address}")
            else:
                bind_address = f"unix://{self.sock_path}{idx}.sock"
            _LOGGER.info("Binding server to: %s", bind_address)

            worker = multiprocessing.Process(
                target=self._run_server_process,
                args=(bind_address,),
            )
            worker.start()
            workers.append(worker)

        # Write server info file
        server_info = ServerInfo.get_default_server_info()
        server_info.metadata[MULTIPROC_KEY] = str(self._process_count)
        server_info.metadata[MAP_MODE_KEY] = MapMode.UnaryMap
        if self.use_tcp:
            server_info.protocol = Protocol.TCP
            server_info.metadata[MULTIPROC_ENDPOINTS] = ",".join(map(str, ports))
        info_server_write(server_info=server_info, info_file=self.server_info_file)

        for worker in workers:
            worker.join()

    def _run_server_process(self, bind_address):
        async def run_server():
            server = grpc.aio.server(options=self._server_options)
            server.add_insecure_port(bind_address)
            map_pb2_grpc.add_MapServicer_to_server(self.servicer, server)

            server_info = ServerInfo.get_default_server_info()
            server_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Mapper]
            server_info.metadata = get_metadata_env(envs=METADATA_ENVS)
            # Add the MULTIPROC metadata using the number of servers to use
            server_info.metadata[MULTIPROC_KEY] = str(self._process_count)
            # Add the MAP_MODE metadata to the server info for the correct map mode
            server_info.metadata[MAP_MODE_KEY] = MapMode.UnaryMap

            await start_async_server(
                server_async=server,
                sock_path=bind_address,
                max_threads=self.max_threads,
                cleanup_coroutines=list(),
                server_info_file=self.server_info_file,
                server_info=server_info,
            )

        aiorun.run(run_server(), use_uvloop=True)
