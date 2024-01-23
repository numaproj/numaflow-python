import inspect

import aiorun
import grpc

from pynumaflow.proto.reducer import reduce_pb2_grpc

from pynumaflow.reducer.servicer.async_servicer import AsyncReduceServicer

from pynumaflow._constants import (
    REDUCE_SOCK_PATH,
    MAX_MESSAGE_SIZE,
    MAX_THREADS,
    _LOGGER,
)

from pynumaflow.reducer._dtypes import (
    ReduceCallable,
    ReduceBuilderClass,
    Reducer,
)

from pynumaflow.shared.server import NumaflowServer, start_async_server


def get_handler(reducer_handler: ReduceCallable, init_args: tuple = (), init_kwargs: dict = None):
    """
    Get the correct handler type based on the arguments passed
    """
    if inspect.isfunction(reducer_handler):
        if len(init_args) > 0 or len(init_kwargs) > 0:
            # if the init_args or init_kwargs are passed, then the reducer_handler
            # can only be of class Reducer type
            raise TypeError("Cannot pass function handler with init args or kwargs")
        # return the function handler
        return reducer_handler
    elif issubclass(reducer_handler, Reducer):
        # if handler is type of Class Reducer, create a new instance of
        # a ReducerBuilderClass
        return ReduceBuilderClass(reducer_handler, init_args, init_kwargs)
    else:
        raise TypeError("Invalid type passed")


class ReduceAsyncServer(NumaflowServer):
    """
    Class for a new Reduce Server instance.
    """

    def __init__(
        self,
        reducer_handler: ReduceCallable,
        init_args: tuple = (),
        init_kwargs: dict = None,
        sock_path=REDUCE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        """
        Create a new grpc Reduce Server instance.
        A new servicer instance is created and attached to the server.
        The server instance is returned.
        Args:
        reducer_instance: The reducer instance to be used for Reduce UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to number of processors x4
        server_type: The type of server to be used
        """
        if init_kwargs is None:
            init_kwargs = {}
        self.reducer_handler = get_handler(reducer_handler, init_args, init_kwargs)
        self.sock_path = f"unix://{sock_path}"
        self.max_message_size = max_message_size
        self.max_threads = max_threads

        self._server_options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
        ]
        # Get the servicer instance for the async server
        self.servicer = AsyncReduceServicer(self.reducer_handler)

    def start(self):
        """
        Starter function for the Async server class, need a separate caller
        so that all the async coroutines can be started from a single context
        """
        _LOGGER.info(
            "Starting Async Reduce Server",
        )
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
        server = grpc.aio.server()
        server.add_insecure_port(self.sock_path)
        reduce_servicer = self.servicer
        reduce_pb2_grpc.add_ReduceServicer_to_server(reduce_servicer, server)
        await start_async_server(server, self.sock_path, self.max_threads, self._server_options)
