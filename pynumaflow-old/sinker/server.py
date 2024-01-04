import logging
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterator, Iterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    SINK_SOCK_PATH,
    MAX_MESSAGE_SIZE,
)
from pynumaflow.info.server import get_sdk_version, write as info_server_write
from pynumaflow.info.types import ServerInfo, Protocol, Language, SERVER_INFO_FILE_PATH
from pynumaflow.sinker import Responses, Datum, Response
from pynumaflow.sinker._dtypes import SinkCallable
from pynumaflow.sinker.proto import sink_pb2_grpc, sink_pb2
from pynumaflow.types import NumaflowServicerContext

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)


_PROCESS_COUNT = multiprocessing.cpu_count()
MAX_THREADS = int(os.getenv("MAX_THREADS", 0)) or (_PROCESS_COUNT * 4)


def datum_generator(request_iterator: Iterable[sink_pb2.SinkRequest]) -> Iterable[Datum]:
    for d in request_iterator:
        datum = Datum(
            keys=list(d.keys),
            sink_msg_id=d.id,
            value=d.value,
            event_time=d.event_time.ToDatetime(),
            watermark=d.watermark.ToDatetime(),
        )
        yield datum


class Sinker(sink_pb2_grpc.SinkServicer):
    """
    Provides an interface to write a Sinker
    which will be exposed over gRPC.

    Args:
        handler: Function callable following the type signature of SinkCallable
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x 4

    Example invocation:
    >>> from typing import List
    >>> from pynumaflow.sinker import Datum, Responses, Response, Sinker
    >>> def my_handler(datums: Iterator[Datum]) -> Responses:
    ...   responses = Responses()
    ...   for msg in datums:
    ...     responses.append(Response.as_success(msg.id))
    ...   return responses
    >>> grpc_server = Sinker(handler=my_handler)
    >>> grpc_server.start()
    """

    def __init__(
        self,
        handler: SinkCallable,
        sock_path=SINK_SOCK_PATH,
        max_message_size=MAX_MESSAGE_SIZE,
        max_threads=MAX_THREADS,
    ):
        self.__sink_handler: SinkCallable = handler
        self.sock_path = f"unix://{sock_path}"
        self._max_message_size = max_message_size
        self._max_threads = max_threads

        self._server_options = [
            ("grpc.max_send_message_length", self._max_message_size),
            ("grpc.max_receive_message_length", self._max_message_size),
        ]

    def SinkFn(
        self, request_iterator: Iterator[sink_pb2.SinkRequest], context: NumaflowServicerContext
    ) -> sink_pb2.SinkResponse:
        """
        Applies a sink function to a list of datum elements.
        The pascal case function name comes from the proto sink_pb2_grpc.py file.
        """
        # if there is an exception, we will mark all the responses as a failure
        datum_iterator = datum_generator(request_iterator)
        try:
            rspns = self.__sink_handler(datum_iterator)
        except Exception as err:
            err_msg = "UDSinkError: %r" % err
            _LOGGER.critical(err_msg, exc_info=True)
            rspns = Responses()
            for _datum in datum_iterator:
                rspns.append(Response.as_failure(_datum.id, err_msg))

        responses = []
        for rspn in rspns:
            responses.append(
                sink_pb2.SinkResponse.Result(id=rspn.id, success=rspn.success, err_msg=rspn.err)
            )

        return sink_pb2.SinkResponse(results=responses)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> sink_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto sink_pb2_grpc.py file.
        """
        return sink_pb2.ReadyResponse(ready=True)

    def start(self) -> None:
        """
        Starts the gRPC server on the given UNIX socket with given max threads.
        """
        server = grpc.server(
            ThreadPoolExecutor(max_workers=self._max_threads), options=self._server_options
        )
        sink_pb2_grpc.add_SinkServicer_to_server(Sinker(self.__sink_handler), server)
        server.add_insecure_port(self.sock_path)
        server.start()
        serv_info = ServerInfo(
            protocol=Protocol.UDS,
            language=Language.PYTHON,
            version=get_sdk_version(),
        )
        info_server_write(server_info=serv_info, info_file=SERVER_INFO_FILE_PATH)

        _LOGGER.info(
            "GRPC Server listening on: %s with max threads: %s", self.sock_path, self._max_threads
        )
        server.wait_for_termination()
