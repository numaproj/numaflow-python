from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow._constants import (
    _LOGGER,
    ERR_UDF_EXCEPTION_STRING,
)
from pynumaflow.proto.sideinput import sideinput_pb2_grpc, sideinput_pb2
from pynumaflow.shared.server import exit_on_error
from pynumaflow.sideinput._dtypes import RetrieverCallable
from pynumaflow.types import NumaflowServicerContext


class SideInputServicer(sideinput_pb2_grpc.SideInputServicer):
    def __init__(
        self,
        handler: RetrieverCallable,
    ):
        self.__retrieve_handler: RetrieverCallable = handler

    def RetrieveSideInput(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> sideinput_pb2.SideInputResponse:
        """
        Applies a sideinput function for a retrieval request.
        The pascal case function name comes from the proto sideinput_pb2_grpc.py file.
        """
        # if there is an exception, we will mark all the responses as a failure
        try:
            rspn = self.__retrieve_handler()
        except BaseException as err:
            err_msg = f"{ERR_UDF_EXCEPTION_STRING}: {repr(err)}"
            _LOGGER.critical(err_msg, exc_info=True)
            exit_on_error(context, err_msg)
            return

        return sideinput_pb2.SideInputResponse(value=rspn.value, no_broadcast=rspn.no_broadcast)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> sideinput_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto sideinput_pb2_grpc.py file.
        """
        return sideinput_pb2.ReadyResponse(ready=True)
