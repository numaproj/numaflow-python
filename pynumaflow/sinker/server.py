import logging
import multiprocessing
import os
from collections.abc import Iterator, Iterable

from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow.sinker._dtypes import Responses, Datum, Response
from pynumaflow.sinker._dtypes import SinkCallable
from pynumaflow.proto.sinker import sink_pb2_grpc, sink_pb2
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
    This class is used to create a new grpc Sink servicer instance.
    It implements the SinkServicer interface from the proto sink.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: SinkCallable,
    ):
        self.__sink_handler: SinkCallable = handler

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
