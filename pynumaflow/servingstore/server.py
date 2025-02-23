from pynumaflow._constants import (
    NUM_THREADS_DEFAULT,
    MAX_MESSAGE_SIZE,
    _LOGGER,
    UDFType,
    MAX_NUM_THREADS,
    SERVING_STORE_SOCK_PATH,
    SERVING_STORE_SERVER_INFO_FILE_PATH,
)
from pynumaflow.info.types import ServerInfo, MINIMUM_NUMAFLOW_VERSION, ContainerType
from pynumaflow.servingstore._dtypes import ServingStoreCallable
from pynumaflow.servingstore.servicer.servicer import SyncServingStoreServicer
from pynumaflow.shared import NumaflowServer
from pynumaflow.shared.server import sync_server_start


class ServingStoreServer(NumaflowServer):
    """
    Class for a new Serving Store instance.
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

            def put(self, datum: PutDatum):
                req_id = datum.id
                print("Received Put request for ", req_id)
                if req_id not in self.store:
                    self.store[req_id] = []

                cur_payloads = self.store[req_id]
                for x in datum.payloads:
                    cur_payloads.append(Payload(x.origin, x.value))
                self.store[req_id] = cur_payloads

            def get(self, datum: GetDatum) -> StoredResult:
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

    def __init__(
        self,
        serving_store_instance: ServingStoreCallable,
        sock_path=SERVING_STORE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=NUM_THREADS_DEFAULT,
        server_info_file=SERVING_STORE_SERVER_INFO_FILE_PATH,
    ):
        self.sock_path = f"unix://{sock_path}"
        self.max_threads = min(max_threads, MAX_NUM_THREADS)
        self.max_message_size = max_message_size
        self.server_info_file = server_info_file

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]

        self.serving_store_instance = serving_store_instance
        self.servicer = SyncServingStoreServicer(serving_store_instance)

    def start(self):
        """
        Starts the Synchronous gRPC server on the given UNIX socket with given max threads.
        """
        # Get the servicer instance based on the server type
        serving_store_servicer = self.servicer

        _LOGGER.info(
            "Serving Store GRPC Server listening on: %s with max threads: %s",
            self.sock_path,
            self.max_threads,
        )

        serv_info = ServerInfo.get_default_server_info()
        serv_info.minimum_numaflow_version = MINIMUM_NUMAFLOW_VERSION[ContainerType.Serving]
        # Start the server
        sync_server_start(
            servicer=serving_store_servicer,
            bind_address=self.sock_path,
            max_threads=self.max_threads,
            server_info_file=self.server_info_file,
            server_options=self._server_options,
            udf_type=UDFType.ServingStore,
            server_info=serv_info,
        )
