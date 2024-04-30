from collections.abc import Iterator, Iterable

from google.protobuf import empty_pb2 as _empty_pb2
from pynumaflow._constants import _LOGGER
from pynumaflow.shared.server import exit_on_error
from pynumaflow.sinker._dtypes import Datum
from pynumaflow.sinker._dtypes import SyncSinkCallable
from pynumaflow.proto.sinker import sink_pb2_grpc, sink_pb2
from pynumaflow.sinker.servicer.utils import build_sink_response
from pynumaflow.types import NumaflowServicerContext


def datum_generator(request_iterator: Iterable[sink_pb2.SinkRequest]) -> Iterable[Datum]:
    for d in request_iterator:
        datum = Datum(
            keys=list(d.keys),
            sink_msg_id=d.id,
            value=d.value,
            event_time=d.event_time.ToDatetime(),
            watermark=d.watermark.ToDatetime(),
            headers=dict(d.headers),
        )
        yield datum


class SyncSinkServicer(sink_pb2_grpc.SinkServicer):
    """
    This class is used to create a new grpc Sink servicer instance.
    It implements the SinkServicer interface from the proto sink.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(
        self,
        handler: SyncSinkCallable,
    ):
        self.__sink_handler: SyncSinkCallable = handler

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
        except BaseException as err:
            err_msg = f"UDSinkError: {repr(err)}"
            _LOGGER.critical(err_msg, exc_info=True)
            exit_on_error(context, err_msg)
            return

        responses = []
        for rspn in rspns:
            responses.append(build_sink_response(rspn))

        return sink_pb2.SinkResponse(results=responses)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> sink_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto sink_pb2_grpc.py file.
        """
        return sink_pb2.ReadyResponse(ready=True)
