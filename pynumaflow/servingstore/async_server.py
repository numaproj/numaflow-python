import aiorun
import grpc

from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
    NUM_THREADS_DEFAULT,
    MAX_NUM_THREADS, SERVING_STORE_SOCK_PATH, SERVING_STORE_SERVER_INFO_FILE_PATH,
)
from pynumaflow.info.types import ServerInfo, ContainerType, MINIMUM_NUMAFLOW_VERSION
from pynumaflow.proto.serving import store_pb2_grpc
from pynumaflow.servingstore._dtypes import ServingStoreCallable
from pynumaflow.servingstore.servicer.async_servicer import AsyncServingStoreServicer
from pynumaflow.shared.server import NumaflowServer, start_async_server


class ServingStoreAsyncServer(NumaflowServer):
    """
    Class for a new Async Serving store Server instance.
    """

    def __init__(
        self,
        serving_store_instance: ServingStoreCallable,
        sock_path=SERVING_STORE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=SERVING_STORE_SERVER_INFO_FILE_PATH,
    ):
        """
        Class for a new Async Serving Store server.
        Args:
            serving_store_instance: The serving store instance to be used
            sock_path: The UNIX socket path to be used for the server
            max_message_size: The max message size in bytes the server can receive and send
            max_threads: The max number of threads to be spawned;

        Example invocation:
            import datetime
            from pynumaflow.servingstore import Response, ServingStoreServer, ServingStorer

            class InMemoryStore(ServingStorer):
                def __init__(self):
                    self.store = {}

                async def put(self, datum: PutDatum):
                    req_id = datum.id
                    print("Received Put request for ", req_id)
                    if req_id not in self.store:
                        self.store[req_id] = []

                    cur_payloads = self.store[req_id]
                    for x in datum.payloads:
                        cur_payloads.append(Payload(x.origin, x.value))
                    self.store[req_id] = cur_payloads

                async def get(self, datum: GetDatum) -> StoredResult:
                    req_id = datum.id
                    print("Received Get request for ", req_id)
                    resp = []
                    if req_id in self.store:
                        resp = self.store[req_id]
                    return StoredResult(id_=req_id, payloads=resp)

            if __name__ == "__main__":
                grpc_server = ServingStoreServer(InMemoryStore())
                grpc_server.start()

        """
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.serving_store_instance = serving_store_instance
        self.servicer = AsyncServingStoreServicer(serving_store_instance)

    def start(self):
        """
        Starter function for the Async server class, need a separate caller
        so that all the async coroutines can be started from a single context
        """
        aiorun.run(self.aexec(), use_uvloop=True)

    async def aexec(self):
        """
        Starts the Async gRPC server on the given UNIX socket with given max threads
        """
        # As the server is async, we need to create a new server instance in the
        # same thread as the event loop so that all the async calls are made in the
        # same context
        # Create a new async server instance and add the servicer to it
        server = grpc.aio.server(options=self._server_options)
        server.add_insecure_port(self.sock_path)
        store_servicer = self.servicer
        store_pb2_grpc.add_ServingStoreServicer_to_server(store_servicer, server)

        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Serving]
        # Start the async server
        await start_async_server(
            server_async=server,
            sock_path=self.sock_path,
            max_threads=self.max_threads,
            cleanup_coroutines=list(),
            server_info_file=self.server_info_file,
            server_info=serv_info,
        )
