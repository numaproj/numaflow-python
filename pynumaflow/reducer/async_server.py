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
    REDUCE_SERVER_INFO_FILE_PATH,
)

from pynumaflow.reducer._dtypes import (
    ReduceCallable,
    _ReduceBuilderClass,
    Reducer,
)

from pynumaflow.shared.server import NumaflowServer, checkInstance, start_async_server


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
    elif not checkInstance(reducer_handler, Reducer) and issubclass(reducer_handler, Reducer):
        # if handler is type of Class Reducer, create a new instance of
        # a ReducerBuilderClass
        return _ReduceBuilderClass(reducer_handler, init_args, init_kwargs)
    else:
        _LOGGER.error("Invalid Type: please provide the handler or the class name")
        raise TypeError("Inavlid Type: please provide the handler or the class name")


class ReduceAsyncServer(NumaflowServer):
    """
    Class for a new Reduce Server instance.
    A new servicer instance is created and attached to the server.
    The server instance is returned.
    Args:
        reducer_handler: The reducer instance to be used for Reduce UDF
        sock_path: The UNIX socket path to be used for the server
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                        defaults to number of processors x4
    Example invocation:
        import os
        from collections.abc import AsyncIterable
        from pynumaflow.reducer import Messages, Message, Datum, Metadata,
        ReduceAsyncServer, Reducer

        class ReduceCounter(Reducer):
            def __init__(self, counter):
                self.counter = counter

            async def handler(
                self, keys: list[str], datums: AsyncIterable[Datum], md: Metadata
            ) -> Messages:
                interval_window = md.interval_window
                self.counter = 0
                async for _ in datums:
                    self.counter += 1
                msg = (
                    f"counter:{self.counter} interval_window_start:{interval_window.start} "
                    f"interval_window_end:{interval_window.end}"
                )
                return Messages(Message(str.encode(msg), keys=keys))

        async def reduce_handler(keys: list[str],
                                datums: AsyncIterable[Datum],
                                md: Metadata) -> Messages:
            interval_window = md.interval_window
            counter = 0
            async for _ in datums:
                counter += 1
            msg = (
                f"counter:{counter} interval_window_start:{interval_window.start} "
                f"interval_window_end:{interval_window.end}"
            )
            return Messages(Message(str.encode(msg), keys=keys))

        if __name__ == "__main__":
            invoke = os.getenv("INVOKE", "func_handler")
            if invoke == "class":
                # Here we are using the class instance as the reducer_instance
                # which will be used to invoke the handler function.
                # We are passing the init_args for the class instance.
                grpc_server = ReduceAsyncServer(ReduceCounter, init_args=(0,))
            else:
                # Here we are using the handler function directly as the reducer_instance.
                grpc_server = ReduceAsyncServer(reduce_handler)
            grpc_server.start()

    """

    def __init__(
        self,
        reducer_handler: ReduceCallable,
        init_args: tuple = (),
        init_kwargs: dict = None,
        sock_path=REDUCE_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
        server_info_file=REDUCE_SERVER_INFO_FILE_PATH,
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
        self.server_info_file = server_info_file

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
        await start_async_server(
            server, self.sock_path, self.max_threads, self._server_options, self.server_info_file
        )
