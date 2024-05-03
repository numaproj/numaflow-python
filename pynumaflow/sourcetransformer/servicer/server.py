from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow.shared.server import exit_on_error
from pynumaflow.sourcetransformer import Datum
from pynumaflow.sourcetransformer._dtypes import SourceTransformCallable
from pynumaflow.proto.sourcetransformer import transform_pb2
from pynumaflow.proto.sourcetransformer import transform_pb2_grpc
from pynumaflow.types import NumaflowServicerContext
from pynumaflow._constants import _LOGGER


class SourceTransformServicer(transform_pb2_grpc.SourceTransformServicer):
    """
    This class is used to create a new grpc SourceTransform servicer instance.
    It implements the SourceTransformServicer interface from the proto transform.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(self, handler: SourceTransformCallable, multiproc: bool = False):
        self.__transform_handler: SourceTransformCallable = handler
        # This indicates whether the grpc server attached is multiproc or not
        self.multiproc = multiproc

    def SourceTransformFn(
        self, request: transform_pb2.SourceTransformRequest, context: NumaflowServicerContext
    ) -> transform_pb2.SourceTransformResponse:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the generated transform_pb2_grpc.py file.
        """

        # proto repeated field(keys) is of type google._upb._message.RepeatedScalarContainer
        # we need to explicitly convert it to list
        try:
            msgts = self.__transform_handler(
                list(request.keys),
                Datum(
                    keys=list(request.keys),
                    value=request.value,
                    event_time=request.event_time.ToDatetime(),
                    watermark=request.watermark.ToDatetime(),
                    headers=dict(request.headers),
                ),
            )
        except BaseException as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            # Terminate the current server process due to exception
            exit_on_error(context, repr(err), parent=self.multiproc)
            return

        datums = []
        for msgt in msgts:
            event_time_timestamp = _timestamp_pb2.Timestamp()
            event_time_timestamp.FromDatetime(dt=msgt.event_time)
            datums.append(
                transform_pb2.SourceTransformResponse.Result(
                    keys=list(msgt.keys),
                    value=msgt.value,
                    tags=msgt.tags,
                    event_time=event_time_timestamp,
                )
            )
        return transform_pb2.SourceTransformResponse(results=datums)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> transform_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto transform_pb2_grpc.py file.
        """
        return transform_pb2.ReadyResponse(ready=True)
