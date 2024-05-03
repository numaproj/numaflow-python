from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.mapper._dtypes import MapSyncCallable
from pynumaflow.proto.mapper import map_pb2, map_pb2_grpc
from pynumaflow.mapper.servicer.utils import _map_fn_util
from pynumaflow.types import NumaflowServicerContext


class SyncMapServicer(map_pb2_grpc.MapServicer):
    """
    This class is used to create a new grpc Map Servicer instance.
    It implements the SyncMapServicer interface from the proto map.proto file.
    Provides the functionality for the required rpc methods.
    """

    def __init__(self, handler: MapSyncCallable, multiproc: bool = False):
        self.__map_handler: MapSyncCallable = handler
        # This indicates whether the grpc server attached is multiproc or not
        self.multiproc = multiproc

    def MapFn(
        self, request: map_pb2.MapRequest, context: NumaflowServicerContext
    ) -> map_pb2.MapResponse:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        return _map_fn_util(self.__map_handler, request, context, self.multiproc)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> map_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        return map_pb2.ReadyResponse(ready=True)
